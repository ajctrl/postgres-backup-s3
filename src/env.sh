# Shared connection settings. Database selection is validated by each caller.
fail() {
  echo "Error: $*" >&2
  exit 1
}

: "${POSTGRES_DATABASE:=}"
: "${POSTGRES_DATABASES:=}"
: "${POSTGRES_BACKUP_ALL:=false}"
: "${POSTGRES_DATABASES_EXCLUDE:=}"
: "${POSTGRES_MAINTENANCE_DB:=postgres}"
: "${POSTGRES_HOST:=}"
: "${POSTGRES_PORT:=5432}"
: "${PGDUMP_EXTRA_OPTS:=}"
: "${S3_ENDPOINT:=}"
: "${S3_REGION:=us-west-1}"
: "${S3_PREFIX=backup}"
: "${PASSPHRASE:=}"
: "${BACKUP_KEEP_DAYS:=}"
: "${BACKUP_FILENAME_MODE:=timestamp}"

[ -n "${S3_BUCKET:-}" ] || fail "You need to set S3_BUCKET."
[ -n "${POSTGRES_USER:-}" ] || fail "You need to set POSTGRES_USER."
[ -n "${POSTGRES_PASSWORD:-}" ] || fail "You need to set POSTGRES_PASSWORD."

if [ -z "$POSTGRES_HOST" ]; then
  if [ -n "${POSTGRES_PORT_5432_TCP_ADDR:-}" ]; then
    POSTGRES_HOST=$POSTGRES_PORT_5432_TCP_ADDR
    POSTGRES_PORT=${POSTGRES_PORT_5432_TCP_PORT:-5432}
  else
    fail "You need to set POSTGRES_HOST."
  fi
fi

case "$BACKUP_FILENAME_MODE" in
  timestamp|fixed) ;;
  *) fail "BACKUP_FILENAME_MODE must be timestamp or fixed." ;;
esac

if [ -n "${S3_ACCESS_KEY_ID:-}" ]; then
  export AWS_ACCESS_KEY_ID=$S3_ACCESS_KEY_ID
fi
if [ -n "${S3_SECRET_ACCESS_KEY:-}" ]; then
  export AWS_SECRET_ACCESS_KEY=$S3_SECRET_ACCESS_KEY
fi
export AWS_DEFAULT_REGION=$S3_REGION
export PGPASSWORD=$POSTGRES_PASSWORD
export AWS_PAGER=""

aws_cli() {
  if [ -n "$S3_ENDPOINT" ]; then
    aws --endpoint-url "$S3_ENDPOINT" "$@"
  else
    aws "$@"
  fi
}

# Timestamped keys must retain the original layout, including repeated/leading
# slashes when S3_PREFIX ends in '/' or is explicitly empty.
timestamp_key_prefix="${S3_PREFIX}/"
fixed_key_prefix="${S3_PREFIX%/}"
[ -z "$fixed_key_prefix" ] || fixed_key_prefix="$fixed_key_prefix/"
file_type=".dump"
[ -z "$PASSPHRASE" ] || file_type=".dump.gpg"

# Comma-separated configuration lists; ALL is an ordinary database name.
parse_database_list() {
  jq -cn --arg names "$1" '$names | split(",") | map(gsub("^\\s+|\\s+$"; ""))'
}

# A URI safely represents names containing spaces, quotes, '=' or slashes.
database_uri() {
  jq -nr --arg database "$1" '"postgresql:///" + ($database | @uri)'
}

# A separate leaf name cannot be mistaken for another DB's timestamped dump.
# Encode the directory component so slashes and percent signs remain DB-name data.
fixed_backup_key() {
  jq -nr --arg prefix "$fixed_key_prefix" --arg database "$database" --arg suffix "$file_type" '
    ($database | @uri | if . == "." then "%2E" elif . == ".." then "%2E%2E" else . end) as $directory |
    $prefix + $directory + "/latest" + $suffix'
}

validate_databases() {
  # Reject control characters so database names remain safe in line-based logs.
  jq -e 'type == "array" and length > 0 and all(.[];
    type == "string" and length > 0 and (test("[\\x00-\\x1f\\x7f]") | not))' "$1" >/dev/null \
    || fail "Database names must be nonempty and contain no control characters."
}

# Match the complete DB name plus timestamp, never just a shared DB-name prefix.
# AWS CLI automatically paginates list-objects-v2 when JSON output is used.
list_timestamp_backups() {
  aws_cli s3api list-objects-v2 --bucket "$S3_BUCKET" \
    --prefix "${timestamp_key_prefix}${database}_" --output json > "$work_dir/objects.json" || return 1
  jq --arg prefix "${timestamp_key_prefix}${database}_" '
    [.Contents[]? | select(.Key | startswith($prefix)) |
      select(.Key[($prefix | length):] |
        test("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.dump(\\.gpg)?\\z"))]
    ' "$work_dir/objects.json" > "$work_dir/backups.json"
}

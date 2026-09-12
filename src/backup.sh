#! /bin/sh

set -eu
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
. "$script_dir/env.sh"

# All entrypoints in this container share the same lock, including manual runs.
# Keep the file: unlinking it would let another process lock a different inode.
exec 9>/tmp/postgres-backup-s3.lock
flock -n 9 || fail "Could not acquire backup lock; another backup may already be running."

case "$POSTGRES_BACKUP_ALL" in
  true)
    [ -z "$POSTGRES_DATABASE$POSTGRES_DATABASES" ] \
      || fail "POSTGRES_BACKUP_ALL cannot be combined with POSTGRES_DATABASE or POSTGRES_DATABASES."
    ;;
  false)
    [ -n "$POSTGRES_DATABASE$POSTGRES_DATABASES" ] \
      || fail "Set POSTGRES_DATABASE, POSTGRES_DATABASES, or POSTGRES_BACKUP_ALL=true."
    ;;
  *) fail "POSTGRES_BACKUP_ALL must be true or false." ;;
esac

if [ -n "$POSTGRES_DATABASES_EXCLUDE" ] && [ "$POSTGRES_BACKUP_ALL" != true ]; then
  fail "POSTGRES_DATABASES_EXCLUDE requires POSTGRES_BACKUP_ALL=true."
fi

retention_enabled=false
if [ -n "$BACKUP_KEEP_DAYS" ]; then
  case "$BACKUP_KEEP_DAYS" in
    *[!0-9]*|0*) fail "BACKUP_KEEP_DAYS must be a positive integer without leading zeros." ;;
  esac
  # Limit arithmetic to a useful, portable range (up to 100 years).
  [ "${#BACKUP_KEEP_DAYS}" -le 5 ] && [ "$BACKUP_KEEP_DAYS" -le 36500 ] \
    || fail "BACKUP_KEEP_DAYS must be at most 36500."
  cutoff=$(date -u -d "@$(($(date +%s) - 86400 * BACKUP_KEEP_DAYS))" +%Y-%m-%dT%H:%M:%S)
  retention_enabled=true
fi

work_dir=$(mktemp -d)
trap 'rm -rf "$work_dir"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [ "$BACKUP_FILENAME_MODE" = fixed ]; then
  aws_cli s3api get-bucket-versioning --bucket "$S3_BUCKET" --output json > "$work_dir/versioning.json" \
    || fail "Could not verify S3 bucket versioning."
  jq -e '.Status == "Enabled"' "$work_dir/versioning.json" >/dev/null \
    || fail "Fixed filenames require S3 bucket versioning to be Enabled."
fi

if [ "$POSTGRES_BACKUP_ALL" = true ]; then
  echo "Discovering databases..."
  maintenance_uri=$(database_uri "$POSTGRES_MAINTENANCE_DB")
  psql -X -A -t -v ON_ERROR_STOP=1 -d "$maintenance_uri" \
    -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" \
    -c "SELECT COALESCE(json_agg(datname ORDER BY datname), '[]'::json) FROM pg_database WHERE NOT datistemplate AND datallowconn;" \
    > "$work_dir/databases.json" || fail "Could not discover databases."
else
  if [ -n "$POSTGRES_DATABASES" ]; then
    parse_database_list "$POSTGRES_DATABASES" > "$work_dir/databases.json"
  else
    jq -cn --arg name "$POSTGRES_DATABASE" '[$name]' > "$work_dir/databases.json"
  fi
fi
validate_databases "$work_dir/databases.json"

if [ -n "$POSTGRES_DATABASES_EXCLUDE" ]; then
  parse_database_list "$POSTGRES_DATABASES_EXCLUDE" > "$work_dir/excluded.json"
  validate_databases "$work_dir/excluded.json"
  jq -r --slurpfile excluded "$work_dir/excluded.json" \
    '.[] | select(. as $db | $excluded[0] | index($db)) | "Excluding database: \(.)"' \
    "$work_dir/databases.json"
  jq --slurpfile excluded "$work_dir/excluded.json" \
    '[.[] | select(. as $db | $excluded[0] | index($db) | not)]' \
    "$work_dir/databases.json" > "$work_dir/selected.json"
else
  cp "$work_dir/databases.json" "$work_dir/selected.json"
fi
# Deduplicate while preserving the configured order.
jq 'reduce .[] as $db ([]; if index($db) then . else . + [$db] end)' \
  "$work_dir/selected.json" > "$work_dir/targets.json"
total=$(jq length "$work_dir/targets.json")
[ "$total" -gt 0 ] || fail "No databases selected for backup."

backup_database() {
  timestamp=$(date -u +"%Y-%m-%dT%H:%M:%S") || return 1
  connection_uri=$(database_uri "$database") || return 1
  echo "Creating backup of $database database..."
  # Preserve the existing space-separated option interface, without glob expansion.
  set -f
  pg_dump --format=custom -d "$connection_uri" \
    -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" \
    $PGDUMP_EXTRA_OPTS > "$db_dir/db.dump" || return 1

  if [ "$BACKUP_FILENAME_MODE" = fixed ]; then
    key=$(fixed_backup_key) || return 1
  else
    key="${timestamp_key_prefix}${database}_${timestamp}${file_type}"
  fi
  local_file="$db_dir/db.dump"
  if [ -n "$PASSPHRASE" ]; then
    echo "Encrypting backup of $database..."
    gpg --symmetric --batch --passphrase "$PASSPHRASE" "$local_file" || return 1
    local_file="$local_file.gpg"
  fi
  echo "Uploading backup of $database..."
  aws_cli s3 cp "$local_file" "s3://${S3_BUCKET}/${key}" || return 1
  echo "Backup complete: $database"
}

remove_old_backups() {
  list_timestamp_backups || return 1
  jq --arg cutoff "$cutoff" '[.[] | select(.LastModified < $cutoff)]' \
    "$work_dir/backups.json" > "$work_dir/expired.json" || return 1
  expired_count=$(jq length "$work_dir/expired.json") || return 1
  expired_index=0
  while [ "$expired_index" -lt "$expired_count" ]; do
    expired_key=$(jq -r --argjson i "$expired_index" '.[$i].Key' "$work_dir/expired.json") || return 1
    aws_cli s3 rm "s3://${S3_BUCKET}/${expired_key}" || return 1
    expired_index=$((expired_index + 1))
  done
}

succeeded=0
failed=0
cleanup_failed=0
index=0
while [ "$index" -lt "$total" ]; do
  database=$(jq -r --argjson i "$index" '.[$i]' "$work_dir/targets.json")
  db_dir="$work_dir/$index"
  mkdir "$db_dir"
  if backup_database; then
    succeeded=$((succeeded + 1))
    if [ "$retention_enabled" = true ]; then
      if ! remove_old_backups; then
        echo "Retention cleanup failed: $database" >&2
        cleanup_failed=$((cleanup_failed + 1))
      fi
    fi
  else
    echo "Backup failed: $database" >&2
    failed=$((failed + 1))
  fi
  rm -rf "$db_dir"
  index=$((index + 1))
done
echo "Backup summary: $succeeded succeeded, $failed failed, $cleanup_failed retention cleanups failed."
[ "$failed" -eq 0 ] && [ "$cleanup_failed" -eq 0 ]

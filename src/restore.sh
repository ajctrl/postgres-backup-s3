#! /bin/sh

set -eu
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
. "$script_dir/env.sh"

[ -n "$POSTGRES_DATABASE" ] || fail "Set POSTGRES_DATABASE to the single database to restore."
database=$POSTGRES_DATABASE
version_id=""
timestamp=""
case "$#" in
  0) ;;
  1)
    [ "$BACKUP_FILENAME_MODE" = timestamp ] \
      || fail "Use --version-id ID to restore a previous fixed-name backup."
    timestamp=$1
    jq -ne --arg timestamp "$timestamp" '$timestamp | test("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\z")' >/dev/null \
      || fail "Invalid backup timestamp."
    ;;
  2)
    [ "$1" = --version-id ] && [ -n "$2" ] && [ "$BACKUP_FILENAME_MODE" = fixed ] \
      || fail "--version-id ID requires BACKUP_FILENAME_MODE=fixed."
    version_id=$2
    ;;
  *) fail "Usage: restore.sh [timestamp | --version-id ID]" ;;
esac

work_dir=$(mktemp -d)
trap 'rm -rf "$work_dir"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
jq -cn --arg name "$database" '[$name]' > "$work_dir/database.json"
validate_databases "$work_dir/database.json"

if [ "$BACKUP_FILENAME_MODE" = fixed ]; then
  key=$(fixed_backup_key)
elif [ -n "$timestamp" ]; then
  key="${timestamp_key_prefix}${database}_${timestamp}${file_type}"
else
  echo "Finding latest backup of $database..."
  list_timestamp_backups || fail "Could not list backups."
  key=$(jq -r --arg suffix "$file_type" \
    '[.[] | select(.Key | endswith($suffix))] | sort_by(.Key) | last | .Key // empty' \
    "$work_dir/backups.json")
  [ -n "$key" ] || fail "No backup found for $database."
fi

echo "Fetching backup of $database from S3..."
if [ -n "$version_id" ]; then
  aws_cli s3api get-object --bucket "$S3_BUCKET" --key "$key" \
    --version-id "$version_id" "$work_dir/db${file_type}" >/dev/null
else
  aws_cli s3 cp "s3://${S3_BUCKET}/${key}" "$work_dir/db${file_type}"
fi

if [ -n "$PASSPHRASE" ]; then
  echo "Decrypting backup..."
  gpg --decrypt --batch --passphrase "$PASSPHRASE" "$work_dir/db.dump.gpg" > "$work_dir/db.dump"
fi

echo "Restoring $database from backup..."
connection_uri=$(database_uri "$database")
pg_restore -d "$connection_uri" -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" \
  -U "$POSTGRES_USER" --exit-on-error --clean --if-exists "$work_dir/db.dump"
echo "Restore complete."

#!/bin/sh

set -eu

postgres_version=${1:-}
case "$postgres_version" in
  17|18) ;;
  *)
    echo 'POSTGRES_VERSION must be 17 or 18.' >&2
    exit 1
    ;;
esac

apk add --no-cache "postgresql${postgres_version}-client" gpg gpg-agent ca-certificates

# Fail the image build if PATH resolves a client from a different major version.
for tool in psql pg_dump pg_restore; do
  version=$("$tool" --version)
  case "$version" in
    *" $postgres_version."*) ;;
    *)
      printf 'Expected PostgreSQL %s client, got: %s\n' "$postgres_version" "$version" >&2
      exit 1
      ;;
  esac
done

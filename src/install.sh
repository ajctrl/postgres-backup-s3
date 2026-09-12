#!/bin/sh

set -eu

# Older Alpine images retain their PostgreSQL client major version. Their GPG
# package predates the separate gpg and gpg-agent packages.
case "$(cat /etc/alpine-release)" in
  3.[0-9].*|3.1[0-4].*)
    apk add --no-cache postgresql-client gnupg ca-certificates
    ;;
  *)
    apk add --no-cache postgresql-client gpg gpg-agent ca-certificates
    ;;
esac

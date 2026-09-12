#! /bin/sh

set -eux
set -o pipefail

apk update

# install pg_dump
apk add postgresql-client

# install gpg
apk add gnupg

# Resolve flock's provider across Alpine versions (util-linux or standalone flock).
apk add aws-cli jq 'cmd:flock'

# install go-cron
apk add curl
curl -L https://github.com/ivoronin/go-cron/releases/download/v0.0.5/go-cron_0.0.5_linux_${TARGETARCH}.tar.gz -O
tar xvf go-cron_0.0.5_linux_${TARGETARCH}.tar.gz
rm go-cron_0.0.5_linux_${TARGETARCH}.tar.gz
mv go-cron /usr/local/bin/go-cron
chmod u+x /usr/local/bin/go-cron
apk del curl


# cleanup
rm -rf /var/cache/apk/*

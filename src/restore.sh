#!/bin/sh

exec postgres-backup-s3 restore "$@"

# Introduction
This project provides Docker images to periodically back up one or more PostgreSQL databases to AWS S3, and to restore individual databases as needed.

# Usage
## Backup
```yaml
services:
  postgres:
    image: postgres:17
    environment:
      POSTGRES_USER: user
      POSTGRES_PASSWORD: password

  backup:
    image: bartels/postgres-backup-s3:17
    environment:
      SCHEDULE: '@weekly'     # optional
      BACKUP_KEEP_DAYS: 7     # optional; deletes expired timestamped backups ONLY
      PASSPHRASE: passphrase  # optional
      S3_REGION: region
      S3_ACCESS_KEY_ID: key
      S3_SECRET_ACCESS_KEY: secret
      S3_BUCKET: my-bucket
      S3_PREFIX: backup
      POSTGRES_HOST: postgres
      POSTGRES_DATABASE: dbname
      POSTGRES_USER: user
      POSTGRES_PASSWORD: password
```

- Images are tagged by the major PostgreSQL version supported: `12`, `13`, `14`, `15` or `16` or `17`.
- The `SCHEDULE` variable determines backup frequency. See go-cron schedules documentation [here](http://godoc.org/github.com/robfig/cron#hdr-Predefined_schedules). Omit to run the backup immediately and then exit.
- If `PASSPHRASE` is provided, the backup will be encrypted using GPG.
- Run `docker exec <container name> sh backup.sh` to trigger a backup ad-hoc.
- **`BACKUP_KEEP_DAYS` applies ONLY to timestamped backup files** such as `app_2026-09-12T12:00:00.dump` (including `.dump.gpg`). It does **not** delete fixed-name `latest.dump` / `latest.dump.gpg` files or their S3 version history. See [Backup retention](#backup-retention) for retention rules and versioned-bucket behavior.
- Set `S3_ENDPOINT` if you're using a non-AWS S3-compatible storage provider.

### Multiple databases

Use the same host, port and credentials for every database. List names separated by commas:

```yaml
POSTGRES_DATABASES: "app,analytics,billing"
```

`POSTGRES_DATABASES` takes precedence over the legacy `POSTGRES_DATABASE` when nonempty. Surrounding whitespace in lists is trimmed, empty entries are rejected, and duplicates are backed up once in the configured order. `ALL` is an ordinary database name. Names containing commas can be selected using `POSTGRES_DATABASE` or discovered with all-database mode; control characters in names are rejected.

To discover all databases at the start of every backup run:

```yaml
POSTGRES_BACKUP_ALL: "true"
POSTGRES_MAINTENANCE_DB: postgres       # connection used to query the database list
POSTGRES_DATABASES_EXCLUDE: "scratch,test_db"  # optional, exact names
```

- `POSTGRES_BACKUP_ALL` defaults to `false` and accepts only `true` or `false`. When true, leave both `POSTGRES_DATABASE` and `POSTGRES_DATABASES` unset or empty.
- All-database mode excludes template databases and databases with connections disabled, and includes `postgres`. Remaining databases run in database-name order. Newly created databases are included on the next run.
- `POSTGRES_DATABASES_EXCLUDE` is available only in all-database mode. Exclusions are logged. Empty selections and discovery failures are errors.
- The configured user needs to connect to the maintenance database and read the contents of every target database. Permission failures are reported, not silently skipped.
- Each database is dumped, optionally encrypted, and uploaded before starting the next. Failed dumps/encryption are not uploaded. Failures do not prevent later databases from running; the final summary and exit status report backup and retention failures. When scheduled, this is the backup command's status; the cron process remains running.
- Temporary files are isolated per run and removed on exit. A container-wide `flock` at `/tmp/postgres-backup-s3.lock` prevents overlapping backups, including scheduled and manual runs. It covers discovery, dump, encryption, upload and retention. An overlapping run exits nonzero before accessing PostgreSQL or S3; it does not queue. The lock is released when the processes exit; the lock file is intentionally kept and must not be deleted while backups are running. This lock does not coordinate separate containers; use only one backup writer per S3 destination.
- These are separate database snapshots, not a single consistent snapshot across databases. Role and tablespace definitions are not included; global-object backups using `pg_dumpall --globals-only` remain outside this feature.

### Filenames and S3 versioning

`BACKUP_FILENAME_MODE` defaults to `timestamp`, preserving the existing keys:

```text
backup/app_2026-09-12T10:00:00.dump
backup/analytics_2026-09-12T10:00:15.dump
```

Timestamp mode preserves the original `${S3_PREFIX}/` layout exactly. For example, `S3_PREFIX=backup/` stores keys under `backup//`, and an explicitly empty `S3_PREFIX` stores keys beginning with `/`. Backup, latest restore, timestamp-specific restore and retention all use this same layout, so existing backups remain accessible. Retention also uses the original timestamp prefix after switching to fixed mode.

Timestamps use UTC and are captured before each database dump. To keep a fixed key per database and let S3 retain the versions:

```yaml
POSTGRES_BACKUP_ALL: "true"
BACKUP_FILENAME_MODE: fixed
S3_PREFIX: backup
```

```text
backup/app/latest.dump
backup/analytics/latest.dump
```

Encryption adds `.gpg` in both modes. Fixed mode percent-encodes the database directory name (for example, `a/b` becomes `a%2Fb`) and uses `latest.dump` or `latest.dump.gpg` inside it. This keeps fixed-name backups distinct from timestamped backups even when a database name ends in a timestamp. Backup, latest restore and VersionId restore all use this layout. Use a separate `S3_PREFIX` per PostgreSQL server to avoid collisions between identically named databases.

Fixed-mode keys add a separator only when needed: `S3_PREFIX=backup/` gives `backup/app/latest.dump`, while an empty prefix gives `app/latest.dump`.

The earlier development layout `<database>.dump` for fixed mode is replaced by `<encoded-database>/latest.dump`. Existing objects and their VersionIds are not automatically migrated to the new key. If you used that development layout, retrieve old versions using their original S3 key. Before enabling timestamp cleanup, move any old flat fixed-name backups whose names could be mistaken for timestamped backups; the old layout cannot distinguish those names. Existing timestamp-mode keys and restore commands remain supported.

Fixed mode checks bucket versioning before each run and refuses to upload unless its status is `Enabled`. A failed check is also an error. The check requires bucket-level `s3:GetBucketVersioning` permission, in addition to the usual object upload permissions. S3-compatible providers must support this API and versioned objects to use fixed mode. Keep versioning enabled throughout operation; the initial check cannot prevent an administrator changing it during a run.

### Backup retention

**`BACKUP_KEEP_DAYS` controls deletion of timestamped backup files only. It does not control retention of fixed-name files or S3 version history.**

| Backup object | Deleted by `BACKUP_KEEP_DAYS`? |
| --- | --- |
| `app_2026-09-12T12:00:00.dump` or `.dump.gpg` | Yes, after the retention period and a successful backup of that database. |
| `app/latest.dump` or `app/latest.dump.gpg` | No. |
| S3 object version history | No. Manage it separately with S3 Lifecycle. |

Set `BACKUP_KEEP_DAYS` to a positive integer (1–36500, without leading zeros), or leave it unset to disable cleanup. Age is measured using S3 `LastModified`, with each day equal to 24 hours. Only timestamped backups strictly older than the retention cutoff at run start are deleted, after a successful backup of that database. With `BACKUP_KEEP_DAYS=7`, backups at most seven days old are retained; older ones are eligible for deletion. Other databases and unrelated objects are left alone.

The rule applies to timestamped files in **both timestamp and fixed modes**. Switching to fixed mode does not immediately delete existing timestamped backups: they remain until the configured retention period has passed.

**This is a filename restriction, not a restriction to unversioned S3 buckets.** In a versioned bucket, deleting an expired timestamped file creates a delete marker; its stored versions remain. Use S3 Lifecycle noncurrent version expiration to permanently remove those versions.

To expire old fixed-name versions, configure S3 Lifecycle **noncurrent version expiration**. Its age is measured from when a version becomes noncurrent, not when the backup was created. Keep the current version unexpired to preserve the latest backup. The application does not create or modify lifecycle rules. Existing S3 lifecycle rules still apply; use separate prefixes if timestamped and fixed backups need different rules.

See [S3 versioning](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Versioning.html) and [Lifecycle rules](https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html).

## Restore
> [!CAUTION]
> DATA LOSS! All database objects will be dropped and re-created.

### ... from latest backup
```sh
docker exec <container name> sh restore.sh
```

For a container backing up multiple or all databases, explicitly choose the single existing database to restore. Backup selection settings are ignored during restore:

```sh
docker exec -e POSTGRES_DATABASE=app <container name> sh restore.sh
```

Use the same filename mode, prefix and passphrase as the backup. Timestamp mode lists all pages of objects and selects the newest matching timestamp for that exact database and encryption format. Fixed mode downloads the current version directly.

### ... from specific backup
```sh
docker exec <container name> sh restore.sh <timestamp>
```

The timestamp argument is supported in timestamp mode only.

### ... from an S3 object version

In fixed mode, obtain the object's VersionId from the S3 console's **Show versions** view or `aws s3api list-object-versions`, then run:

```sh
docker exec -e POSTGRES_DATABASE=app <container name> sh restore.sh --version-id '<VersionId>'
```

Previous-version downloads require object-level `s3:GetObjectVersion`; latest downloads require `s3:GetObject`. Listing versions through the API requires bucket-level `s3:ListBucketVersions`. Timestamp-mode restore lookup requires `s3:ListBucket`. Retention cleanup in either filename mode requires `s3:ListBucket` and `s3:DeleteObject`. These are in addition to any encryption/KMS permissions required by your bucket.

Download or decryption failures stop before `pg_restore`. Restore errors return a nonzero status and can leave a partially restored database. To restore a timestamped backup after switching to fixed mode, also pass `-e BACKUP_FILENAME_MODE=timestamp` to `docker exec`.

# Development
## Run regression tests

```sh
python3 -m unittest discover -s tests -v
```

These tests require `jq`, `flock` (util-linux), Python 3 and a POSIX shell. They run the actual scripts with fake PostgreSQL, S3 and GPG commands to cover database selection, upload failures, retention boundaries in both filename modes, fixed-name collisions, and restore behavior without contacting external services. Concurrency tests use real processes and `flock` to verify overlapping runs are rejected during dump, upload and retention, and that later runs can acquire the lock after success or failure.

## Build the image locally
`ALPINE_VERSION` determines Postgres version compatibility. See [`build-and-push-images.yml`](.github/workflows/build-and-push-images.yml) for the latest mapping.
```sh
DOCKER_BUILDKIT=1 docker build --build-arg ALPINE_VERSION=3.14 .
```
## Run a simple test environment with Docker Compose
```sh
cp template.env .env
# fill out your secrets/params in .env
docker compose up -d
```

# Acknowledgements
This project is a fork and re-structuring of @schickling's [postgres-backup-s3](https://github.com/schickling/dockerfiles/tree/master/postgres-backup-s3) and [postgres-restore-s3](https://github.com/schickling/dockerfiles/tree/master/postgres-restore-s3).

## Fork goals
These changes would have been difficult or impossible merge into @schickling's repo or similarly-structured forks.
  - dedicated repository
  - automated builds
  - support multiple PostgreSQL versions
  - backup and restore with one image

## Other changes and features
  - some environment variables renamed or removed
  - uses `pg_dump`'s `custom` format (see [docs](https://www.postgresql.org/docs/10/app-pgdump.html))
  - drop and re-create all database objects on restore
  - backup blobs and all schemas by default
  - no Python 2 dependencies
  - filter backups on S3 by database name
  - support encrypted (password-protected) backups
  - support for restoring from a specific backup by timestamp
  - support for auto-removal of expired timestamped backups in both filename modes

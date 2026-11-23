# PostgreSQL Backup to S3

Back up one or more PostgreSQL databases to AWS S3 or S3-compatible storage, and restore individual databases when needed. Backups stream directly to S3, with optional GPG encryption and no temporary dump files.

The image combines a Go executable, AWS SDK for Go v2, PostgreSQL client tools and GPG. It supports scheduled or manual backups, timestamp-based retention, and fixed filenames backed by S3 versioning. AWS CLI and Python are not required.

- [Quick start](#quick-start)
- [Back up databases](#back-up-databases)
- [Restore a database](#restore-a-database)
- [Storage and retention](#storage-and-retention)
- [Environment variable reference](#environment-variable-reference)
- [Publishing images](#publishing-images)
- [Development](#development)
- [Compatibility](#compatibility)

## Quick start

Use an existing PostgreSQL database and S3 bucket. The database must be reachable from the backup container, and the configured user must be able to read the selected database.

Images support PostgreSQL major-version tags `17` and `18` on `linux/amd64` and `linux/arm64`. Both use Alpine 3.23. Select the tag matching your PostgreSQL major version. This example uses `ghcr.io/ajctrl/postgres-backup-s3:18`; for a fork, use `ghcr.io/<owner>/<repository>:18` in lowercase after [publishing its images](#publishing-images).

Create a `compose.yaml` for the backup service. Replace the example connection and bucket values. This example takes AWS access keys and an optional encryption passphrase from your shell or a `.env` file next to `compose.yaml`:

```yaml
services:
  backup:
    image: ghcr.io/ajctrl/postgres-backup-s3:18
    environment:
      POSTGRES_HOST: postgres.example.com
      POSTGRES_PORT: "5432"
      POSTGRES_DATABASE: app
      POSTGRES_USER: backup
      POSTGRES_PASSWORD: "${POSTGRES_PASSWORD:?Set POSTGRES_PASSWORD}"
      S3_BUCKET: my-backup-bucket
      S3_REGION: us-west-1
      S3_PREFIX: backup
      S3_ACCESS_KEY_ID: "${S3_ACCESS_KEY_ID:?Set S3_ACCESS_KEY_ID}"
      S3_SECRET_ACCESS_KEY: "${S3_SECRET_ACCESS_KEY:?Set S3_SECRET_ACCESS_KEY}"
      SCHEDULE: "0 2 * * *"
      BACKUP_KEEP_DAYS: "7"
      BACKUP_FILENAME_MODE: "${BACKUP_FILENAME_MODE:-timestamp}"
      BACKUP_ALLOW_UNVERSIONED_OVERWRITE: "${BACKUP_ALLOW_UNVERSIONED_OVERWRITE-false}"
      PASSPHRASE: "${PASSPHRASE:-}"
```

For profiles or workload IAM roles, omit the two `S3_*` credential entries and configure the [AWS credential chain](#s3-destination-and-credentials). Set `S3_ENDPOINT` for a non-AWS storage provider.

Start the scheduler, trigger the first backup, and inspect its logs:

```sh
docker compose up -d backup
docker compose exec backup postgres-backup-s3 backup
docker compose logs -f backup
```

The schedule above runs daily at 02:00 UTC by default. Scheduled containers wait until the next scheduled time; they do not back up immediately on startup. The manual command runs immediately. Setting `PASSPHRASE` enables encryption; leaving it empty produces an unencrypted dump. Retention removes eligible timestamped backups after seven days—see [retention rules](#backup-retention).

## Back up databases

### Commands and scheduling

| Command | Behavior |
| --- | --- |
| `postgres-backup-s3 run` | Default container command. Uses `SCHEDULE`, or backs up once and exits when it is unset or empty. |
| `postgres-backup-s3 backup` | Backs up immediately, regardless of `SCHEDULE`. |
| `postgres-backup-s3 restore [generation \| --version-id ID]` | Restores one database; see [restore instructions](#restore-a-database). |

`SCHEDULE` accepts five cron fields (`minute hour day-of-month month day-of-week`), six fields with leading seconds, and descriptors such as `@weekly` or `@every 1h`. For example, `0 0 2 * * *` runs daily at 02:00. Schedules use the container's local timezone, normally UTC; `CRON_TZ=Asia/Tokyo 0 0 2 * * *` selects a timezone explicitly. Backup timestamps always use UTC.

### Select databases

Set `POSTGRES_DATABASE` for one database, or use a comma-separated list:

```yaml
POSTGRES_DATABASES: "app,analytics,billing"
```

`POSTGRES_DATABASES` takes precedence for backup. Lists are trimmed, empty entries are rejected, and duplicates are removed while preserving order. The scheduler validates the selected names and exclusion list at startup. `ALL` is an ordinary name. Use `POSTGRES_DATABASE` or discovery for names containing commas; control characters are rejected.

To discover databases at the start of every run, leave both selection variables empty and set:

```yaml
POSTGRES_BACKUP_ALL: "true"
POSTGRES_MAINTENANCE_DB: postgres
POSTGRES_DATABASES_EXCLUDE: "scratch,test_db"
```

Discovery includes `postgres` and other connectable, non-template databases, in database-name order. Newly created databases are included on the next run. Exclusions match complete names and are valid only in all-database mode. Discovery errors, permission errors and an empty selection fail the run.

### Failure handling and concurrency

Databases run sequentially, using the same host, port and credentials. A dump, encryption or upload failure skips retention for that database and does not prevent later databases from running. The backup command returns nonzero if any backup or retention cleanup fails. The scheduler logs the failure and remains available for subsequent runs.

Before dumping each database, `psql` checks whether the connection is to a standby and whether that database contains UNLOGGED relations. A standby omits UNLOGGED contents: such backups fail by default, before upload or retention. Prefer backing up the primary to preserve those contents. Set `BACKUP_ALLOW_PARTIAL=true` only when their omission is intentional; standby backups then emit a warning. Failure to determine the source state always stops that database's backup. The streamed archive is also checked for UNLOGGED definitions on a detected standby, covering relations added after the initial check.

The dump connection is restricted to the role found by the source check: `target_session_attrs=primary` or `standby` is supplied explicitly in the database URI. This overrides environment/service-file role preferences for `pg_dump`, so a multi-host connection cannot silently switch from a checked primary to a standby. If no server of the verified role is available, that database's backup fails without publication or retention. Discovery and the source check still use the configured connection preferences.

Database discovery and source checks force `PGCLIENTENCODING=UTF8` and reject invalid UTF-8 before decoding JSON. This prevents non-ASCII database names from being silently changed. The override applies only to these metadata queries; `pg_dump` retains its existing encoding behavior.

These metadata connections also set `search_path` to `pg_catalog, pg_temp` after connecting and explicitly qualify built-in functions, catalog relations and types. Functions or aggregates created by other database users cannot replace the source checks or hide databases from discovery through schema search order. This overrides inherited search-path preferences only for metadata queries; other connection options, such as statement timeouts, are preserved.

On PostgreSQL 17 and 18, a non-superuser dump role can read table data while omitting logical replication subscriptions. By default the backup stops before publication or retention if the actual dump reports this omission, including after a `--role` switch. Set `BACKUP_ALLOW_PARTIAL=true` to accept the omission. The strict check runs `pg_dump` with C-locale, uncolored diagnostics.

By default, `pg_dump` waits up to five minutes to acquire its initial table locks. Override this with, for example, `PGDUMP_EXTRA_OPTS=--lock-wait-timeout=30s`. For backups that take longer than 24 hours, increase `BACKUP_DATABASE_TIMEOUT` to cover the expected dump and upload time.

These are independent database snapshots, not one consistent snapshot across databases. Roles and tablespaces are not included; `pg_dumpall --globals-only` is outside this tool's scope.

A container-wide lock at `/tmp/postgres-backup-s3.lock` covers discovery, dump, encryption, upload and retention, and is shared with restore. Overlapping backups or restores fail before accessing PostgreSQL or S3; they do not queue. Processes release the lock when they exit. Do not delete the lock file while either operation is running. Separate containers require a shared lock filesystem or external coordination; use one backup writer per S3 destination.

## Restore a database

> [!CAUTION]
> Restore replaces archived objects in the existing database in a single transaction. The target database keeps its own access privileges (including `CONNECT`), database-specific and role-in-database settings, and database comment. Objects created after the backup can remain. A failed restore rolls back its database changes.

For a clean recovery, prepare an empty target database with the backed-up database name in a separate instance, configure its database-level privileges and settings, then run restore. Verify the data and access rules before directing applications to the recovered instance.

Choose one existing target database using `POSTGRES_DATABASE`, even if the container backs up multiple or all databases. Use the same S3 prefix, filename mode and passphrase as the selected backup. Other database-selection settings are ignored during restore.

### Latest backup

```sh
docker compose exec -e POSTGRES_DATABASE=app backup postgres-backup-s3 restore
```

Timestamp mode selects the matching generation with the newest S3 `LastModified` for that exact database and encryption format, across all listing pages. Equal modification times are resolved by lexicographical generation name; random IDs do not establish creation order within a tied timestamp. Fixed mode downloads the current object directly.

### Specific generation

Specify the full generation name (`UTC timestamp` + `-` + 32 lowercase hexadecimal characters), without `.dump` or `.dump.gpg`. The name appears in the backup completion log and S3 key. This argument is supported only in timestamp mode:

```sh
docker compose exec -e POSTGRES_DATABASE=app -e BACKUP_FILENAME_MODE=timestamp \
  backup postgres-backup-s3 restore 2026-09-12T10:00:00-0123456789abcdef0123456789abcdef
```

### Specific S3 version

For fixed filenames, obtain the VersionId from the S3 console's **Show versions** view or run `aws s3api list-object-versions` outside this image:

```sh
docker compose exec -e POSTGRES_DATABASE=app -e BACKUP_FILENAME_MODE=fixed \
  backup postgres-backup-s3 restore --version-id '<VersionId>'
```

Restore downloads and, if necessary, decrypts into temporary files, then checks the custom archive's table of contents before invoking `pg_restore --single-transaction --exit-on-error --clean --if-exists`. Download, decryption and archive-validation failures stop before database access. Allow disk space for the downloaded file and, for encrypted backups, the decrypted dump as well. Temporary files are removed on normal completion and handled failures.

Backups normally contain object definitions and data. Data-only backups (`--data-only`, `-a`, or section selections without `pre-data`) are always rejected, even with `BACKUP_ALLOW_PARTIAL=true`; schema-only backups require that explicit partial-backup permission. Restore rejects table/sequence data without the required definitions, including historical data-only backups. In particular, every `SEQUENCE SET` entry must have a matching sequence definition: changing an existing sequence with `setval` would not roll back after a later restore failure. Extension configuration tables are permitted when extension definitions are present, but sequence values without their own sequence definitions are rejected even for extensions. The metadata reader supports custom archive versions 1.14 through 1.16; unknown layouts are rejected before database access.

Archives containing data must also contain independently verifiable pre-data object definitions. Session settings and `DATABASE`/`DATABASE PROPERTIES` entries are not evidence: they can also occur in data-only dumps. A database containing only large objects can produce full and data-only archives that cannot reliably be distinguished. Both are rejected, including historical backups. Large objects alongside ordinary object definitions remain supported. The same check runs during backup streaming, so ambiguous archives fail before publication and retention rather than creating a new unusable generation. Empty databases without data remain supported. This structural check does not prove completeness or validate every dependency/SQL statement.

Restore preserves the recorded object owners by default. To create restored objects under the PostgreSQL login user instead, set `RESTORE_NO_OWNER=true` for the restore process:

```bash
docker compose exec -e POSTGRES_DATABASE=app -e RESTORE_NO_OWNER=true \
  backup postgres-backup-s3 restore
```

This passes `--no-owner` to `pg_restore`. It does not suppress privileges: roles referenced by `GRANT`/`REVOKE` may still need to exist. `PGDUMP_EXTRA_OPTS=--no-owner` (or `-O`) is rejected because that option does not remove ownership from a custom-format archive.

Restore also preserves recorded tablespace assignments and table access methods by default. A missing required tablespace causes restore to fail; there is no automatic fallback. To use the restore session's default tablespace and table access method instead, enable these independent restore settings:

```bash
docker compose exec -e POSTGRES_DATABASE=app \
  -e RESTORE_NO_TABLESPACES=true -e RESTORE_NO_TABLE_ACCESS_METHOD=true \
  backup postgres-backup-s3 restore
```

These pass `--no-tablespaces` and `--no-table-access-method` to `pg_restore`. They change object placement/access-method selection; they do not install extensions or remove other dependencies on custom access methods. Both default to `false`, and only lowercase `true`/`false` are accepted. Dump-time `--no-tablespaces` and `--no-table-access-method` are rejected because they have no effect on custom archives. Other ineffective dump options (`--clean`/`-c`, `--create`/`-C`, `--if-exists`, `--disable-triggers`, `--use-set-session-authorization`) are also rejected. Restore still targets an existing database and always uses `--clean --if-exists`; it does not offer database creation, trigger disabling, or session-authorization mode.

`PGDUMP_EXTRA_OPTS` also rejects `--extra-float-digits` (including values that would otherwise be accepted by PostgreSQL) so backups use PostgreSQL's default floating-point output precision. `--binary-upgrade` is rejected because its archives are intended for upgrade utilities, not ordinary restores. Rejected options stop backup before starting subprocesses, uploading objects, or deleting old generations.

DB-level selection (`POSTGRES_DATABASE`, `POSTGRES_DATABASES`, or discovery with `POSTGRES_DATABASES_EXCLUDE`) does not require partial-backup permission. Filtering the contents **inside** a selected database does. Set `BACKUP_ALLOW_PARTIAL=true` explicitly to use table/schema/extension selection or exclusion, `--filter=<file>`, `--section`, `--schema-only`, `--enable-row-security`, large-object/unlogged-data exclusion, or metadata exclusions (`--no-acl`/`--no-privileges`, `--no-comments`, `--no-publications`, `--no-security-labels`, `--no-subscriptions`). Short equivalents such as `-t`, `-n`, `-e`, `-T`, `-N`, `-s`, `-B`, and `-x` have the same requirement.

```bash
docker compose exec -e POSTGRES_DATABASE=app -e BACKUP_ALLOW_PARTIAL=true \
  -e PGDUMP_EXTRA_OPTS='--table=orders' backup postgres-backup-s3 backup
```

Partial backups log a warning: selected objects may lack dependencies, excluded sections can omit constraints/indexes, and row security can limit the saved rows. Permission does not guarantee standalone restore, bypass archive validation, or relax the data-only ban. Use a separate `S3_PREFIX` for partial backups if they should not share fixed keys or retention with normal backups. The flag controls creation; restore still validates the archive independently.

`--filter=-` and empty filter paths are always rejected because `pg_dump` has no connected standard input. A filter file requires partial-backup permission even if its current contents select everything. `--no-synchronized-snapshots` is rejected because neither supported PostgreSQL client provides it. `--no-statistics` is accepted only with `pg_dump` 18 or newer: when requested, the application checks the actual executable with `pg_dump --version` before database discovery or S3 access. A failed/unrecognized version check stops backup. Ordinary options do not require this extra subprocess.

Backups and restores acquire the same nonblocking file lock before accessing S3 or PostgreSQL. A competing operation fails immediately. The lock covers download, decryption and restore, and is inherited by child processes. This protects processes sharing the same lock file (normally in one container); separate containers need a shared lock filesystem or external coordination. It does not pause application writes.

A single-transaction restore holds locks until completion. Databases with many objects may require more PostgreSQL lock resources; use a separate restore database and a planned application cutover when an in-place atomic restore is impractical.

Restore logs report command failure and exit status, but suppress raw `pg_restore` output and PostgreSQL diagnostics. Those messages can contain SQL, row values, or passwords from objects such as USER MAPPING. No detailed SQL logging mode is provided. A failed restore still returns nonzero and rolls back its transaction.

Without Compose, use `docker exec -e POSTGRES_DATABASE=app <container name> postgres-backup-s3 restore`. The [legacy shell entrypoints](#compatibility) also remain available.

## Storage and retention

### Streaming and large backups

With encryption enabled, the backup pipeline is `pg_dump → GPG → S3`; otherwise it is `pg_dump → S3`. Neither plaintext nor encrypted dumps are saved locally. The plaintext `PGDMP` header is checked before forwarding output to GPG or S3; empty or non-custom output fails. The uncompressed table of contents is also checked while bytes continue through the stream, using the same structural rules as restore. The upload completes only after this check, `pg_dump`, and, when enabled, GPG succeed. Validation/producer failures abort multipart uploads and preserve the previous fixed-name backup; retention does not run for that failed database. Data may already have been sent as uncommitted parts when validation fails. This is not a full archive-integrity or restore-success check. S3 failures stop the pipeline.

Uploads buffer parts in memory and send up to two concurrently. The part size stays fixed because the final stream length is unknown in advance. Smaller backups are buffered and sent in one request. Memory usage is proportional to a few parts; larger parts require more memory.

TOC validation also retains object names/types, so databases with many objects need additional metadata memory. It does not buffer SQL definitions or the complete dump.

S3 permits [10,000 parts per multipart upload](https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html). The default limit is **8 MiB × 10,000 = 78.125 GiB per backup**, measured after compression and any encryption, not by database size on disk.

| `S3_UPLOAD_PART_SIZE_MB` | Part size | Maximum uploaded data per backup |
| --- | --- | --- |
| `8` (default) | 8 MiB | 78.125 GiB |
| `64` | 64 MiB | 625 GiB |
| `128` | 128 MiB | 1,250 GiB |

Set a larger value before starting a larger backup, allowing headroom for its expected uploaded size. Parts do not grow automatically; the default limit remains 78.125 GiB. Each database is uploaded separately, so this limit applies to each database backup, including when backing up all databases.

At the start of each backup run, the log shows the configured part size and the multipart size limit in bytes and GiB. If the stream exceeds that limit, the error reports the limit and tells you to increase `S3_UPLOAD_PART_SIZE_MB` and retry (or reduce the backup size if the part size is already at its maximum). The upload fails and attempts to abort its multipart data instead of publishing a truncated backup. A stream exactly at the limit succeeds only if the producer finishes successfully.

S3 connections time out after 60 seconds without send or receive progress. Active transfers may run longer. After SDK retries are exhausted, the operation fails and cleanup releases the backup lock. S3 lifecycle rules can remove incomplete multipart uploads left by abrupt termination such as `SIGKILL` or a host crash.

### Filenames and versioning

Use a separate `S3_PREFIX` per PostgreSQL server to avoid collisions between databases with identical names.

| `BACKUP_FILENAME_MODE` | Example key with `S3_PREFIX=backup` | Requirements |
| --- | --- | --- |
| `timestamp` (default) | `backup/app/2026-09-12T10:00:00-0123456789abcdef0123456789abcdef.dump` | UTC timestamp captured before each database dump plus a cryptographically random 128-bit generation ID. Existing generation keys cannot be overwritten. |
| `fixed` | `backup/app.dump` | Uses the fixed key when versioning is confirmed or unversioned overwrite is explicitly allowed. Otherwise uses a timestamped key. |

Encryption adds `.gpg` in either mode. Both modes percent-encode the database name: `a/b` becomes `a%2Fb`. Timestamp mode stores backups in a database directory; fixed mode stores `<encoded-database>.dump` directly under the prefix. This keeps fixed keys separate from timestamped backups.

Prefix separator handling remains unchanged:

| `S3_PREFIX` | Timestamp example | Fixed example |
| --- | --- | --- |
| `backup` | `backup/app/2026-09-12T10:00:00-0123456789abcdef0123456789abcdef.dump` | `backup/app.dump` |
| `backup/` | `backup//app/2026-09-12T10:00:00-0123456789abcdef0123456789abcdef.dump` | `backup/app.dump` |
| Empty | `/app/2026-09-12T10:00:00-0123456789abcdef0123456789abcdef.dump` | `app.dump` |

In fixed mode, each run calls `GetBucketVersioning` before the first dump and applies the result to every selected database. Custom S3 endpoints, including IDrive e2, use the same API. Timestamp mode skips this check.

| Versioning check in fixed mode | `BACKUP_ALLOW_UNVERSIONED_OVERWRITE` unset or `false` | `BACKUP_ALLOW_UNVERSIONED_OVERWRITE=true` |
| --- | --- | --- |
| `Enabled` | Fixed key | Fixed key |
| `Suspended` or successful response without `Status` | Log the reason and use timestamped keys | Warn and overwrite fixed keys |
| Check failed (for example, `AccessDenied` or a network error) | Log the error and use timestamped keys | Warn and overwrite fixed keys |

After a timestamped upload succeeds, the log prints the actual key and a command to restore that exact generation in the backup container. After a fallback, an argument-free restore in fixed mode still reads the previous fixed object, if one exists. Use the logged command with the same S3 settings and passphrase. Upload errors count as backup failures in either mode.

Timestamp mode sends `If-None-Match: *` when publishing either a single-request upload or a completed multipart upload. A collision fails the backup and skips retention for that database; multipart data is aborted. The provider must enforce these conditional writes. No existence-check-then-write sequence is used.

The wall clock is checked immediately before every publication/deletion HTTP attempt, including SDK retries after backoff, and after upload. If it is earlier than that database backup's start time, the run stops with an error and performs no further publication or deletion. Guard failures are not retryable. Multipart abort requests remain allowed so uploaded parts can be cleaned up. A rollback during an already-sent S3 request cannot retract that request or earlier completed database backups; the post-upload check still prevents retention for the failed backup.

### Backup retention

**`BACKUP_KEEP_DAYS` deletes timestamped backups only, in either filename mode.** Switching to fixed mode leaves existing generation-ID backups subject to the same retention period. Legacy timestamp-only keys are excluded.

| Object | Retention behavior |
| --- | --- |
| `app/2026-09-12T10:00:00-0123456789abcdef0123456789abcdef.dump` or `.dump.gpg` | Eligible after a successful backup of that database, when strictly older than the cutoff. |
| `app.dump` or `.dump.gpg` | Never deleted by application retention. |
| S3 version history | Never deleted by application retention. Use S3 lifecycle rules. |

Age uses S3 `LastModified`, with a day equal to 24 hours. After each successful database backup, the cutoff is the server time from the first listing response's HTTP `Date` header minus `BACKUP_KEEP_DAYS`. Later listing pages and the host clock do not move that cutoff. Backups exactly on the cutoff are retained, and the newly published key is explicitly excluded from deletion even if its timestamp appears old. Matching includes the complete database name and valid timestamp-ID suffix; other databases and unrelated objects are left alone.

If the first listing response has a missing or invalid `Date`, retention deletes nothing for that database. The new backup remains saved, but the run reports a retention failure and returns nonzero for monitoring. S3-compatible providers and intervening proxies must supply an accurate, valid HTTP `Date`; there is no fallback to the host clock. Disabling retention does not require that header.

Deleting an expired timestamped object in a versioned bucket creates a delete marker; stored versions remain. Use lifecycle **noncurrent version expiration** to remove old versions of timestamped or fixed-name backups. Noncurrent age starts when a version becomes noncurrent, not when the backup was created. Keep the current fixed-name version unexpired to retain the latest backup.

The application does not create or modify lifecycle rules. Existing rules still apply; use separate prefixes when backups need different policies. See [S3 versioning](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Versioning.html) and [lifecycle rules](https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html).

### S3 permissions

In addition to upload permissions and any bucket encryption/KMS permissions, grant the permissions needed by the selected operations:

| Operation | Permission |
| --- | --- |
| Confirm versioning for fixed-name backups | `s3:GetBucketVersioning` on the bucket. If denied, the run uses timestamped keys by default. |
| Restore the current object | `s3:GetObject` on backup objects |
| Restore a specific VersionId | `s3:GetObjectVersion` on backup objects |
| Find the latest timestamped backup | `s3:ListBucket` on the bucket |
| Retention cleanup | `s3:ListBucket` on the bucket and `s3:DeleteObject` on backup objects |
| List versions externally to choose a VersionId | `s3:ListBucketVersions` on the bucket |

## Environment variable reference

Set these variables on the **backup container** through Compose `environment:`, `docker run -e` or `docker exec -e`. Every application-specific setting and compatibility alias is listed below. `Unset` means no configured value; empty strings have the same effect unless noted. Application booleans use lowercase `"true"` or `"false"`.

Image-publishing secrets belong to the GitHub repository and are listed under [publishing images](#publishing-images). Build and integration-test settings are listed under [development settings](#development-settings).

### PostgreSQL connection and database selection

| Variable | Default | Required / behavior |
| --- | --- | --- |
| `POSTGRES_HOST` | Unset | Required for backup and restore, unless the legacy host variable below is set. PostgreSQL hostname or address. |
| `POSTGRES_PORT` | `5432` | PostgreSQL port when `POSTGRES_HOST` is set. |
| `POSTGRES_USER` | Unset | Required for backup and restore. PostgreSQL login user. |
| `POSTGRES_PASSWORD` | Unset | Required and nonempty for backup and restore. Passed to PostgreSQL tools as `PGPASSWORD`. |
| `POSTGRES_DATABASE` | Unset | Single database to back up; required for restore. For backup, `POSTGRES_DATABASES` takes precedence. |
| `BACKUP_DATABASE_TIMEOUT` | `24h` | Positive Go duration for one database's source check, dump, encryption and upload combined. Discovery has its own interval of the same length. After a database times out, its retention is skipped and the run continues with the next database. |
| `POSTGRES_DATABASES` | Unset | Comma-separated backup targets. Surrounding whitespace is trimmed and duplicates are removed. |
| `POSTGRES_BACKUP_ALL` | `false` | Discover all connectable, non-template databases. When `true`, both database-selection variables above must be empty. |
| `POSTGRES_DATABASES_EXCLUDE` | Unset | Comma-separated, exact database names to exclude. Valid only with `POSTGRES_BACKUP_ALL=true`. |
| `POSTGRES_MAINTENANCE_DB` | `postgres` | Database used to discover targets in all-database mode. |
| `PGDUMP_EXTRA_OPTS` | Unset | Validated `pg_dump` options, split on spaces, tabs and newlines. Compression is supported without partial permission; content/metadata selection requires `BACKUP_ALLOW_PARTIAL=true`. Data-only/statistics-only mode, sections without `pre-data`, stdin filters, `--no-synchronized-snapshots`, `--extra-float-digits`, `--binary-upgrade`, and ineffective custom-archive options listed under Restore are rejected, as are output path, format, connection overrides, positional arguments, help/version and abbreviated long names. `--no-statistics` requires a verified client version of at least 18. Shell quoting, expansion and globbing are not evaluated. |
| `BACKUP_ALLOW_PARTIAL` | `false` | `true` permits explicit selection/exclusion of contents inside a database and logs a warning. Does not affect which databases are selected. Does not bypass archive validation or permit data-only/statistics-only backups. Only lowercase `true`/`false` are accepted. |
| `RESTORE_NO_OWNER` | `false` | `true` passes `--no-owner` to `pg_restore`, making restored objects owned by the login user. `false` preserves recorded owners. Only these two values are accepted; privileges are still restored. |
| `RESTORE_NO_TABLESPACES` | `false` | `true` passes `--no-tablespaces` to `pg_restore`, placing objects in the restore session's default tablespace. `false` preserves recorded assignments and fails if a required tablespace is missing. Only `true`/`false` are accepted. |
| `RESTORE_NO_TABLE_ACCESS_METHOD` | `false` | `true` passes `--no-table-access-method` to `pg_restore`, using the restore session's default table access method. `false` preserves recorded methods. Only `true`/`false` are accepted; this does not remove other extension/access-method dependencies. |
| `POSTGRES_PORT_5432_TCP_ADDR` | Unset | Legacy Docker-link fallback for `POSTGRES_HOST`, used only when `POSTGRES_HOST` is empty. |
| `POSTGRES_PORT_5432_TCP_PORT` | `5432` | Port used with the legacy host fallback; in that case it replaces `POSTGRES_PORT`. |

A backup requires `POSTGRES_DATABASE`, `POSTGRES_DATABASES`, or `POSTGRES_BACKUP_ALL=true`. Restore always targets `POSTGRES_DATABASE` alone.

### Scheduling, encryption and retention

| Variable | Default | Required / behavior |
| --- | --- | --- |
| `SCHEDULE` | Unset | Omit to back up immediately and exit. Otherwise use a five- or six-field cron expression or a descriptor such as `@weekly`. Applies to the `run` command only; manual `backup` runs immediately. |
| `PASSPHRASE` | Unset | Nonempty enables streaming GPG encryption. Use the same passphrase when restoring an encrypted backup. Empty selects unencrypted `.dump` files. Passed to GPG through a dedicated pipe, never as a process argument, and removed from child environments. Line breaks and NUL bytes are rejected to prevent silent truncation by GPG. |
| `BACKUP_KEEP_DAYS` | Unset | Omit to disable retention. Otherwise an integer from `1` to `36500`, without leading zeros. Deletes only expired timestamped backups after a successful backup of that database. |
| `BACKUP_FILENAME_MODE` | `timestamp` | `timestamp` or `fixed`. Fixed mode checks bucket versioning before choosing the key format for the run. |
| `BACKUP_ALLOW_UNVERSIONED_OVERWRITE` | `false` | In fixed mode, `true` allows overwriting the fixed key even when versioning is suspended, absent or unverifiable. The previous backup may be lost. Only lowercase `true` and `false` are valid. An explicitly empty value is an error. |

### S3 destination and credentials

| Variable | Default | Required / behavior |
| --- | --- | --- |
| `S3_BUCKET` | Unset | Required bucket name for backup and restore. |
| `S3_REGION` | `us-west-1` | Region used by the application. Takes precedence over AWS region environment variables and profile settings. |
| `S3_PREFIX` | `backup` | Object-key prefix. An explicitly empty value is preserved; timestamp keys then start with `/`. Trailing slashes are preserved according to the [filename-mode rules](#filenames-and-versioning). |
| `S3_ENDPOINT` | Unset | Custom `http://` or `https://` S3 endpoint. Uses path-style addressing. Do not include URL credentials, a query or a fragment. Takes precedence over SDK endpoint settings. |
| `S3_ACCESS_KEY_ID` | Unset | Overrides `AWS_ACCESS_KEY_ID` / `AWS_ACCESS_KEY` when nonempty. Optional when using the default credential chain. |
| `S3_SECRET_ACCESS_KEY` | Unset | Overrides `AWS_SECRET_ACCESS_KEY` / `AWS_SECRET_KEY` when nonempty. |
| `S3_SESSION_TOKEN` | Unset | Optional temporary-credential token; overrides `AWS_SESSION_TOKEN` when nonempty. |
| `S3_UPLOAD_PART_SIZE_MB` | `8` | Integer from `5` to `5120`, in **MiB**. Larger parts increase the maximum streamed backup size and memory usage. See [streaming and large backups](#streaming-and-large-backups). |
| `S3_S3V4` | `no` | Legacy compatibility variable, ignored by the Go implementation. Requests already use Signature Version 4. |
| `AWS_ACCESS_KEY_ID` | Unset | Standard AWS access key, used when no S3 access-key override is set. |
| `AWS_SECRET_ACCESS_KEY` | Unset | Standard AWS secret key, used when no S3 secret-key override is set. |
| `AWS_SESSION_TOKEN` | Unset | Session token for temporary AWS credentials. |
| `AWS_ACCESS_KEY` | Unset | Legacy fallback for `AWS_ACCESS_KEY_ID`. |
| `AWS_SECRET_KEY` | Unset | Legacy fallback for `AWS_SECRET_ACCESS_KEY`. |

If any `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY` or `S3_SESSION_TOKEN` override is set, a complete access-key/secret-key pair must be available from the `S3_*` and/or `AWS_*` environment variables. These overrides are not merged with credentials from profiles or IAM roles. With no S3 overrides, the AWS SDK uses its default credential chain.

### Additional SDK and client settings

The AWS SDK and PostgreSQL/GPG tools also read their own environment variables. Common operational settings are listed here; the linked upstream references describe further dependency-specific options and their version-dependent support.

| Variable | Default / behavior |
| --- | --- |
| `AWS_PROFILE`, `AWS_DEFAULT_PROFILE` | Select a shared AWS profile; `AWS_PROFILE` takes precedence. The default profile is `default`. Mount the corresponding credential/config files into the container. |
| `AWS_SHARED_CREDENTIALS_FILE` | Shared credential file path; normally `~/.aws/credentials`. |
| `AWS_CONFIG_FILE` | Shared configuration file path; normally `~/.aws/config`. |
| `AWS_REGION`, `AWS_DEFAULT_REGION` | Standard SDK region variables; the application supplies `S3_REGION` explicitly, so use `S3_REGION` to change its region. |
| `AWS_CA_BUNDLE` | Optional PEM CA bundle for AWS HTTPS connections. The file must exist inside the container. |
| `AWS_MAX_ATTEMPTS` | SDK retry-attempt limit, including the initial request. |
| `AWS_RETRY_MODE` | SDK retry strategy, such as `standard` or `adaptive`. |
| `AWS_REQUEST_CHECKSUM_CALCULATION`, `AWS_RESPONSE_CHECKSUM_VALIDATION` | SDK checksum policy: `WHEN_SUPPORTED` or `WHEN_REQUIRED`. |
| `AWS_ENDPOINT_URL`, `AWS_ENDPOINT_URL_S3` | SDK-wide or S3-specific endpoint override when `S3_ENDPOINT` is unset. |
| `AWS_IGNORE_CONFIGURED_ENDPOINT_URLS` | Set to `true` to ignore SDK environment/profile endpoint overrides. Does not override an explicit `S3_ENDPOINT`. |
| `AWS_ROLE_ARN`, `AWS_WEB_IDENTITY_TOKEN_FILE`, `AWS_ROLE_SESSION_NAME` | Web-identity role configuration; commonly supplied by a workload platform. The token file must be available inside the container. |
| `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, `AWS_CONTAINER_CREDENTIALS_FULL_URI` | Container credential-provider endpoints, normally supplied by the hosting platform. |
| `AWS_CONTAINER_AUTHORIZATION_TOKEN`, `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE` | Authorization for a container credential endpoint, when required by the platform. |
| `AWS_EC2_METADATA_DISABLED` | Set to `true` to disable EC2 instance-metadata credential discovery. |
| `PGCONNECT_TIMEOUT` | PostgreSQL connection timeout in seconds; inherited by `psql`, `pg_dump` and `pg_restore`. |
| `PGSSLMODE` | PostgreSQL TLS mode, for example `require` or `verify-full`. |
| `PGSSLROOTCERT`, `PGSSLCERT`, `PGSSLKEY` | PostgreSQL CA certificate, client certificate and private-key paths inside the container. |
| `PGOPTIONS`, `PGAPPNAME` | Extra PostgreSQL session options and application name. |
| `PGHOST`, `PGPORT`, `PGUSER`, `PGDATABASE`, `PGPASSWORD` | Do not use these as substitutes for the required `POSTGRES_*` settings: connection arguments are supplied explicitly, and `PGPASSWORD` is overwritten with `POSTGRES_PASSWORD`. |
| `TZ` | Local timezone for scheduling, normally UTC in the image. `CRON_TZ=...` inside `SCHEDULE` overrides the schedule timezone; `CRON_TZ` is not a separate application environment variable. Backup timestamps remain UTC. |
| `TMPDIR` | Restore temporary-file directory; `/tmp` when unset. Streaming backups do not save dumps there. The backup lock remains at `/tmp/postgres-backup-s3.lock`. |
| `GNUPGHOME` | GPG configuration/keyring directory; normally `~/.gnupg`. |
| `HTTP_PROXY`, `HTTPS_PROXY`, `NO_PROXY` | HTTP/HTTPS proxy configuration for the SDK's HTTP client; lowercase equivalents are also supported. |
| `SSL_CERT_FILE`, `SSL_CERT_DIR` | Optional certificate-file/directory overrides for Go's system certificate pool on Linux. |

See the [AWS SDK environment-variable reference](https://docs.aws.amazon.com/sdkref/latest/guide/environment-variables.html) and the [PostgreSQL client environment-variable reference](https://www.postgresql.org/docs/17/libpq-envars.html). PostgreSQL options depend on the client major version in the selected image.

## Publishing images

The [GitHub Actions workflow](.github/workflows/build-and-push-images.yml) runs regression tests and Docker integration tests before building the PostgreSQL image matrix. Pushes to `master` publish to GHCR by default. Pull requests build and test without registry login or publishing.

### GHCR

The workflow uses the lowercase `github.repository` value for `ghcr.io/<owner>/<repository>:<postgres-version>`. Forks therefore publish under their own repository automatically. No Docker Hub account or manually created token is required.

1. On a fork, enable workflows in the repository's **Actions** tab.
2. Push to `master` and wait for the test and publish jobs to succeed.
3. For anonymous pulls, change the new package's visibility to **Public** in its package settings. GHCR packages are [private on first publication](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#pushing-container-images), even when the repository is public.

```sh
docker pull ghcr.io/ajctrl/postgres-backup-s3:18
```

### Optional Docker Hub publishing

To publish the same build to Docker Hub as well, add both credentials under **Settings → Secrets and variables → Actions → Repository secrets**. The destination becomes `docker.io/<DOCKERHUB_USERNAME>/<repository>:<postgres-version>`, with lowercase names.

If either credential is missing, Docker Hub login and publishing are skipped. Configured but invalid credentials fail the job; correct or remove them to proceed.

| Setting | Where it comes from | Required / behavior |
| --- | --- | --- |
| `GITHUB_TOKEN` | Automatically supplied by GitHub Actions as `secrets.GITHUB_TOKEN` | Used to publish to GHCR. **Do not create or register it manually.** The workflow grants `packages: write` to the publishing job. |
| `DOCKERHUB_USERNAME` | Optional repository secret | Docker Hub username. Additional Docker Hub publishing is enabled only when this and `DOCKERHUB_TOKEN` are both nonempty. |
| `DOCKERHUB_TOKEN` | Optional repository secret | Docker Hub access token with push permission. Not needed for GHCR-only publishing. |
| `GITHUB_REPOSITORY`, `GITHUB_OUTPUT` | Built-in Actions environment variables | Supplied automatically for image naming and step outputs; no manual configuration. |
| `POSTGRES_VERSION`, `DOCKERHUB_ENABLED` | Internal step environment variables | Computed by the workflow from its matrix, event and secrets; not user-configurable repository settings. |

There are no required repository **Variables** entries. Only the publishing job receives `packages: write` for its automatic `GITHUB_TOKEN`.

## Development

Use Go 1.26 or later on Linux. The race detector requires a C compiler; production builds disable CGO.

### Tests

```sh
go test -count=1 -race ./...
go vet ./...
```

`-count=1` prevents stale results: the CLI built by the tests and externally built Docker images are not tracked as test dependencies. Regression tests use local HTTP servers and fake PostgreSQL/GPG commands, without AWS credentials.

With PostgreSQL server tools installed, run the archive and restore-placement tests as an unprivileged user (adjust the binary directory for PostgreSQL 17 or 18):

```sh
PG_TEST_REQUIRED=true PG_TEST_BINDIR=/usr/lib/postgresql/17/bin go test -count=1 -race -v ./internal/backup \
  -run 'Test(ArchiveSchema|RestorePlacement)WithPostgreSQL'
```

These tests create and remove private local clusters. Restore-placement checks use an in-memory download store and verify missing-tablespace failure/rollback, explicit placement/access-method overrides, and exact floating-point round trips. They skip when server tools are unavailable or the test runs as root.

The local PostgreSQL tests also verify that large-object-only archives are rejected before modifying existing large objects, while full dumps containing ordinary object definitions and large objects pass structural validation. They start a private streaming standby to exercise UNLOGGED rejection and explicit permission, verify UTF-8 discovery under a LATIN1 client environment, and check that a real USER MAPPING restore failure does not disclose its password. The restore tests require `pg_basebackup` and the `postgres_fdw` extension in addition to server/client tools. Pipeline tests cover TOC validation without losing buffered/read-ahead bytes and prevent publication/retention after ambiguous archives. Retention tests cover host clocks ahead/behind S3, missing/invalid server dates, pagination, and protection of the newly published key.

CI compiles the native PostgreSQL test binary and runs it as the unprivileged `postgres` user in both `postgres:17` and `postgres:18` containers. `PG_TEST_REQUIRED=true` makes unavailable tools, root execution, or a missing required extension fail the tests instead of skipping them. These tests are required before image publication. They also verify role changes between preflight and dump, including failure when the verified role disappears. HTTP regression tests cover rollback between SDK attempts for single-request publication, multipart completion, and retention deletion, plus successful retries without rollback.

Native tests also install attacker-owned function/aggregate overloads in `public`, verify that source checks never execute them as the backup user, and ensure discovery cannot omit a database through those overloads. They cover the default search path, role/database defaults and `PGOPTIONS`, and check that unrelated session options remain effective.

For actual PostgreSQL, GPG and Silo round trips, use a local Docker daemon:

```sh
docker build -t postgres-backup-s3:go-migration .
go test -count=1 -tags=integration -v -timeout=15m ./tests/integration
```

The integration suite creates and removes disposable containers and a network. It checks plaintext restore, encrypted fixed-name version restore, repeated backups with a surviving GPG agent, and a multipart encrypted backup with an unwritable temporary directory. The latter restore checks row count and a checksum of all values. Tests use isolated credentials and do not contact AWS.

The S3 fixture uses [Silo's distroless image](https://github.com/pgsty/silo/releases/tag/RELEASE.2026-10-18T00-00-00Z), pinned to the release's multi-platform digest. It starts with `server /data` and retains the `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` environment variables supported by Silo. Readiness and bucket setup use the test's AWS SDK client, so no shell, `curl`, or `mc` is needed inside the Silo container.

### Local executable

Install PostgreSQL client tools and GPG, set the runtime environment variables, then run:

```sh
go build -o /tmp/postgres-backup-s3 ./cmd/postgres-backup-s3
/tmp/postgres-backup-s3 backup
```

### Docker builds

The multistage build produces a stripped, static Go executable. The compiler and module cache stay in the build stage. The default runtime uses Alpine 3.23 and PostgreSQL client 18.

```sh
docker build -t postgres-backup-s3:local .
docker buildx build --platform linux/amd64,linux/arm64 --build-arg POSTGRES_VERSION=17 .
```

`POSTGRES_VERSION` explicitly selects the PostgreSQL client package, independently of `ALPINE_VERSION`. Only `17` and `18` are accepted. The image build verifies that `psql`, `pg_dump` and `pg_restore` resolve to the requested major version:

| PostgreSQL tag | Client package | Alpine version |
| --- | --- | --- |
| `17` | `postgresql17-client` | `3.23` |
| `18` | `postgresql18-client` | `3.23` |

PostgreSQL 12–16 images are no longer built, tested or updated. Previously published tags may remain in registries, but are unsupported. Both supported images install `gpg` and `gpg-agent` from Alpine 3.23; see [Alpine's support table](https://alpinelinux.org/releases/).

### Development settings

| Variable / argument | Default | Scope |
| --- | --- | --- |
| `BACKUP_TEST_IMAGE` | `postgres-backup-s3:go-migration` | Environment variable for the Docker integration test. |
| `POSTGRES_TEST_IMAGE` | `postgres:18` | Environment variable for the Docker integration test's PostgreSQL image; match the backup image's client major version. |
| `S3_TEST_IMAGE` | Silo distroless image pinned by digest in `tests/integration/docker_test.go` | Environment variable for the Docker integration test's S3 fixture; overrides must support `server /data` and the same credential variables. |
| `ALPINE_VERSION` | `3.23` | Docker **build argument**, not a runtime environment variable. Selects the runtime base OS; does not select the PostgreSQL client version. |
| `POSTGRES_VERSION` | `18` | Docker **build argument**, not a runtime environment variable. Selects client major `17` or `18`. |
| `GO_VERSION` | `1.26` | Docker **build argument** selecting the builder's Go version. |
| `BUILDPLATFORM`, `TARGETOS`, `TARGETARCH` | Supplied by Docker BuildKit | Build-platform inputs; select target architectures with `docker buildx build --platform`. |

### Repository Compose fixture

The included [docker-compose.yaml](docker-compose.yaml) builds a PostgreSQL 18 backup image on Alpine 3.23 and starts a PostgreSQL 18 development server. It still needs an S3 bucket and credentials:

```sh
cp template.env .env
# Fill in S3_REGION, S3_BUCKET and credentials in .env.
docker compose up -d --build
docker compose exec backup postgres-backup-s3 backup
```

## Compatibility

Supported PostgreSQL image tags are now `17` and `18`; updates for tags `12`–`16` have ended. The default build and development Compose server have moved to PostgreSQL 18. Custom builds must use `POSTGRES_VERSION` to select the client major version; changing `ALPINE_VERSION` no longer selects it.

The Go implementation preserves existing runtime configuration, `.dump` / `.dump.gpg` formats and shell entrypoints:

| Legacy entrypoint | Equivalent command |
| --- | --- |
| `sh run.sh` | `postgres-backup-s3 run` |
| `sh backup.sh` | `postgres-backup-s3 backup` |
| `sh restore.sh [arguments]` | `postgres-backup-s3 restore [arguments]` |

Requests use Signature Version 4; `S3_S3V4` is accepted but has no effect. `PGDUMP_EXTRA_OPTS` remains a whitespace-separated argument list, not shell code. Only explicitly supported options are now accepted; output/format/connection overrides and abbreviated long names no longer work. Data-only/statistics-only backups, section selections without `pre-data`, and dump-time `--no-owner`/`-O` are no longer accepted. Historical archives with data but missing required object definitions, including sequence definitions, are rejected before database access; custom archive layouts outside versions 1.14 through 1.16 are also rejected. Passphrases containing line breaks are no longer accepted. Concurrent backup/restore attempts now fail on the shared lock, and restore errors roll back database changes.

`PGDUMP_EXTRA_OPTS` now also rejects `--extra-float-digits`, `--binary-upgrade`, `--no-tablespaces`, `--no-table-access-method`, `--clean`/`-c`, `--create`/`-C`, `--if-exists`, `--disable-triggers`, and `--use-set-session-authorization` (including bundled short forms). Existing configurations containing these options fail before backup and must remove them. Tablespace/access-method overrides are available only through `RESTORE_NO_TABLESPACES` and `RESTORE_NO_TABLE_ACCESS_METHOD` during restore; both default to `false`. Previously rounded floating-point values cannot be recovered from those backups; take a new backup from the original database.

Content/metadata selection options now require `BACKUP_ALLOW_PARTIAL=true`, including schema-only dumps and ACL exclusions; selecting whole databases does not. Stdin filters and `--no-synchronized-snapshots` are no longer accepted. `--no-statistics` fails preflight on PostgreSQL 17 or an unrecognized client. Data-bearing archives without independently verifiable object definitions are rejected during backup and restore, including ambiguous large-object-only archives even if originally produced as full dumps. This applies to historical archives too; no compatibility bypass is provided.

Every selected database now requires a successful `psql` source check, including explicit single-database backups. Standby backups containing UNLOGGED relations fail unless `BACKUP_ALLOW_PARTIAL=true`. Metadata queries override `PGCLIENTENCODING` with UTF-8. Retention now uses S3 server time after publication instead of the host's run-start time and fails without deleting anything when the first listing response lacks a valid HTTP `Date`. Raw `pg_restore` diagnostics are no longer present in logs; callers still receive failure status.

Dump connections now require the primary/standby role observed by preflight, overriding `PGTARGETSESSIONATTRS` or service-file preferences for that connection. A failover to the other role causes backup failure when no matching server remains. S3 publication/deletion retries are stopped if the wall clock has moved backwards since backup start.

Metadata queries now override inherited `search_path` settings with `pg_catalog, pg_temp`; custom functions, aggregates or catalog lookalikes in application schemas are no longer used for database discovery or source checks.

The key layout is now `<encoded-database>.dump` for fixed mode and `<encoded-database>/<timestamp>-<generation-ID>.dump` for timestamp mode, relative to the configured prefix (with `.gpg` added for encryption). Previous `<encoded-database>/latest.dump` fixed objects and flat `<database>_<timestamp>.dump` timestamped objects are not migrated or selected by restore or retention. Retrieve those backups and their VersionIds using their original S3 keys. Timestamp-only restore arguments are no longer accepted. Timestamp-only directory keys are also excluded from automatic restore and retention. Restore requires the complete new generation name; fixed-mode `--version-id ID` is unchanged.

Deployments that previously updated fixed keys on `AccessDenied` now write timestamped generations by default. To keep updating the fixed key, enable bucket versioning and grant the backup credentials `s3:GetBucketVersioning`, or set `BACKUP_ALLOW_UNVERSIONED_OVERWRITE=true` to accept possible loss of the previous backup.

## Acknowledgements and license

This project builds on [alexanderbartels/postgres-backup-s3](https://github.com/alexanderbartels/postgres-backup-s3) and Johannes Schickling's [postgres-backup-s3](https://github.com/schickling/dockerfiles/tree/master/postgres-backup-s3) and [postgres-restore-s3](https://github.com/schickling/dockerfiles/tree/master/postgres-restore-s3).

[MIT License](LICENSE.txt).

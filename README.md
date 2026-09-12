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

Images use PostgreSQL major-version tags `12` through `17` and support `linux/amd64` and `linux/arm64`. Select the tag matching your PostgreSQL major version. This example uses `ghcr.io/ajctrl/postgres-backup-s3:17`; for a fork, use `ghcr.io/<owner>/<repository>:17` in lowercase after [publishing its images](#publishing-images).

Create a `compose.yaml` for the backup service. Replace the example connection and bucket values. This example takes AWS access keys and an optional encryption passphrase from your shell or a `.env` file next to `compose.yaml`:

```yaml
services:
  backup:
    image: ghcr.io/ajctrl/postgres-backup-s3:17
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
| `postgres-backup-s3 restore [timestamp \| --version-id ID]` | Restores one database; see [restore instructions](#restore-a-database). |

`SCHEDULE` accepts five cron fields (`minute hour day-of-month month day-of-week`), six fields with leading seconds, and descriptors such as `@weekly` or `@every 1h`. For example, `0 0 2 * * *` runs daily at 02:00. Schedules use the container's local timezone, normally UTC; `CRON_TZ=Asia/Tokyo 0 0 2 * * *` selects a timezone explicitly. Backup timestamps always use UTC.

### Select databases

Set `POSTGRES_DATABASE` for one database, or use a comma-separated list:

```yaml
POSTGRES_DATABASES: "app,analytics,billing"
```

`POSTGRES_DATABASES` takes precedence for backup. Lists are trimmed, empty entries are rejected, and duplicates are removed while preserving order. `ALL` is an ordinary name. Use `POSTGRES_DATABASE` or discovery for names containing commas; control characters are rejected.

To discover databases at the start of every run, leave both selection variables empty and set:

```yaml
POSTGRES_BACKUP_ALL: "true"
POSTGRES_MAINTENANCE_DB: postgres
POSTGRES_DATABASES_EXCLUDE: "scratch,test_db"
```

Discovery includes `postgres` and other connectable, non-template databases, in database-name order. Newly created databases are included on the next run. Exclusions match complete names and are valid only in all-database mode. Discovery errors, permission errors and an empty selection fail the run.

### Failure handling and concurrency

Databases run sequentially, using the same host, port and credentials. A dump, encryption or upload failure skips retention for that database and does not prevent later databases from running. The backup command returns nonzero if any backup or retention cleanup fails. The scheduler logs the failure and remains available for subsequent runs.

These are independent database snapshots, not one consistent snapshot across databases. Roles and tablespaces are not included; `pg_dumpall --globals-only` is outside this tool's scope.

A container-wide lock at `/tmp/postgres-backup-s3.lock` covers discovery, dump, encryption, upload and retention. Overlapping scheduled or manual backups fail before accessing PostgreSQL or S3; they do not queue. Processes release the lock when they exit. Do not delete the lock file while backups are running. The lock does not coordinate separate containers, so use one backup writer per S3 destination.

## Restore a database

> [!CAUTION]
> Restore drops and re-creates database objects. A failed restore can leave the target database partially restored.

Choose one existing target database using `POSTGRES_DATABASE`, even if the container backs up multiple or all databases. Use the same S3 prefix, filename mode and passphrase as the selected backup. Other database-selection settings are ignored during restore.

### Latest backup

```sh
docker compose exec -e POSTGRES_DATABASE=app backup postgres-backup-s3 restore
```

Timestamp mode selects the newest matching timestamp for that exact database and encryption format, across all S3 listing pages. Fixed mode downloads the current object directly.

### Specific timestamp

The timestamp argument is supported only in timestamp mode. Set that mode explicitly when restoring an older timestamped backup after switching to fixed filenames:

```sh
docker compose exec -e POSTGRES_DATABASE=app -e BACKUP_FILENAME_MODE=timestamp \
  backup postgres-backup-s3 restore 2026-09-12T10:00:00
```

### Specific S3 version

For fixed filenames, obtain the VersionId from the S3 console's **Show versions** view or run `aws s3api list-object-versions` outside this image:

```sh
docker compose exec -e POSTGRES_DATABASE=app -e BACKUP_FILENAME_MODE=fixed \
  backup postgres-backup-s3 restore --version-id '<VersionId>'
```

Restore downloads and, if necessary, decrypts into temporary files before invoking `pg_restore --exit-on-error --clean --if-exists`. Download or decryption failures stop before changing the database. Allow disk space for the downloaded file and, for encrypted backups, the decrypted dump as well. Temporary files are removed on normal completion and handled failures.

Without Compose, use `docker exec -e POSTGRES_DATABASE=app <container name> postgres-backup-s3 restore`. The [legacy shell entrypoints](#compatibility) also remain available.

## Storage and retention

### Streaming and large backups

With encryption enabled, the backup pipeline is `pg_dump → GPG → S3`; otherwise it is `pg_dump → S3`. Neither plaintext nor encrypted dumps are saved locally. The upload completes only after `pg_dump` and, when enabled, GPG succeed. Producer failures abort the multipart upload and preserve the previous fixed-name backup. S3 failures stop the pipeline.

Uploads buffer parts in memory and send up to two concurrently. The part size stays fixed because the final stream length is unknown in advance. Smaller backups are buffered and sent in one request. Memory usage is proportional to a few parts; larger parts require more memory.

S3 permits [10,000 parts per multipart upload](https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html). The default limit is **8 MiB × 10,000 = 78.125 GiB per backup**, measured after compression and any encryption, not by database size on disk.

| `S3_UPLOAD_PART_SIZE_MB` | Part size | Maximum uploaded data per backup |
| --- | --- | --- |
| `8` (default) | 8 MiB | 78.125 GiB |
| `64` | 64 MiB | 625 GiB |

Set a larger value before starting a larger backup, allowing headroom for its expected uploaded size. Parts do not grow automatically. Exceeding 10,000 parts fails and aborts the upload instead of publishing a truncated backup.

S3 connections time out after 60 seconds without send or receive progress. Active transfers may run longer. After SDK retries are exhausted, the operation fails and cleanup releases the backup lock. S3 lifecycle rules can remove incomplete multipart uploads left by abrupt termination such as `SIGKILL` or a host crash.

### Filenames and versioning

Use a separate `S3_PREFIX` per PostgreSQL server to avoid collisions between databases with identical names.

| `BACKUP_FILENAME_MODE` | Example key with `S3_PREFIX=backup` | Requirements |
| --- | --- | --- |
| `timestamp` (default) | `backup/app_2026-09-12T10:00:00.dump` | UTC timestamp captured before each database dump. |
| `fixed` | `backup/app/latest.dump` | Bucket versioning must be `Enabled`. |

Encryption adds `.gpg` in either mode. Fixed mode percent-encodes the database directory: `a/b` becomes `a%2Fb`. This keeps fixed keys distinct from other databases' timestamped backups.

Prefix handling preserves existing object keys:

| `S3_PREFIX` | Timestamp example | Fixed example |
| --- | --- | --- |
| `backup` | `backup/app_2026-09-12T10:00:00.dump` | `backup/app/latest.dump` |
| `backup/` | `backup//app_2026-09-12T10:00:00.dump` | `backup/app/latest.dump` |
| Empty | `/app_2026-09-12T10:00:00.dump` | `app/latest.dump` |

Fixed mode verifies versioning before every backup run and rejects disabled, suspended or unverified versioning. S3-compatible providers must support this API and versioned objects. Keep versioning enabled throughout operation; the initial check cannot prevent later administrative changes.

### Backup retention

**`BACKUP_KEEP_DAYS` deletes timestamped backups only, in either filename mode.** Switching to fixed mode leaves existing timestamped backups subject to the same retention period.

| Object | Retention behavior |
| --- | --- |
| `app_2026-09-12T10:00:00.dump` or `.dump.gpg` | Eligible after a successful backup of that database, when strictly older than the cutoff. |
| `app/latest.dump` or `.dump.gpg` | Never deleted by application retention. |
| S3 version history | Never deleted by application retention. Use S3 lifecycle rules. |

Age uses S3 `LastModified`, with a day equal to 24 hours and the cutoff captured at run start. Backups exactly on the cutoff are retained. Matching includes the complete database name and timestamp suffix; other databases and unrelated objects are left alone.

Deleting an expired timestamped object in a versioned bucket creates a delete marker; stored versions remain. Use lifecycle **noncurrent version expiration** to remove old versions of timestamped or fixed-name backups. Noncurrent age starts when a version becomes noncurrent, not when the backup was created. Keep the current fixed-name version unexpired to retain the latest backup.

The application does not create or modify lifecycle rules. Existing rules still apply; use separate prefixes when backups need different policies. See [S3 versioning](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Versioning.html) and [lifecycle rules](https://docs.aws.amazon.com/AmazonS3/latest/userguide/intro-lifecycle-rules.html).

### S3 permissions

In addition to upload permissions and any bucket encryption/KMS permissions, grant the permissions needed by the selected operations:

| Operation | Permission |
| --- | --- |
| Check versioning for fixed-name backups | `s3:GetBucketVersioning` on the bucket |
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
| `POSTGRES_DATABASES` | Unset | Comma-separated backup targets. Surrounding whitespace is trimmed and duplicates are removed. |
| `POSTGRES_BACKUP_ALL` | `false` | Discover all connectable, non-template databases. When `true`, both database-selection variables above must be empty. |
| `POSTGRES_DATABASES_EXCLUDE` | Unset | Comma-separated, exact database names to exclude. Valid only with `POSTGRES_BACKUP_ALL=true`. |
| `POSTGRES_MAINTENANCE_DB` | `postgres` | Database used to discover targets in all-database mode. |
| `PGDUMP_EXTRA_OPTS` | Unset | Additional `pg_dump` arguments, split on spaces, tabs and newlines. Shell quoting, expansion and globbing are not evaluated. |
| `POSTGRES_PORT_5432_TCP_ADDR` | Unset | Legacy Docker-link fallback for `POSTGRES_HOST`, used only when `POSTGRES_HOST` is empty. |
| `POSTGRES_PORT_5432_TCP_PORT` | `5432` | Port used with the legacy host fallback; in that case it replaces `POSTGRES_PORT`. |

A backup requires `POSTGRES_DATABASE`, `POSTGRES_DATABASES`, or `POSTGRES_BACKUP_ALL=true`. Restore always targets `POSTGRES_DATABASE` alone.

### Scheduling, encryption and retention

| Variable | Default | Required / behavior |
| --- | --- | --- |
| `SCHEDULE` | Unset | Omit to back up immediately and exit. Otherwise use a five- or six-field cron expression or a descriptor such as `@weekly`. Applies to the `run` command only; manual `backup` runs immediately. |
| `PASSPHRASE` | Unset | Nonempty enables streaming GPG encryption. Use the same passphrase when restoring an encrypted backup. Empty selects unencrypted `.dump` files. |
| `BACKUP_KEEP_DAYS` | Unset | Omit to disable retention. Otherwise an integer from `1` to `36500`, without leading zeros. Deletes only expired timestamped backups after a successful backup of that database. |
| `BACKUP_FILENAME_MODE` | `timestamp` | `timestamp` or `fixed`. Fixed mode requires bucket versioning to be `Enabled`; version-history retention is managed by S3 lifecycle rules. |

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
docker pull ghcr.io/ajctrl/postgres-backup-s3:17
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

For actual PostgreSQL, GPG and MinIO round trips, use a local Docker daemon:

```sh
docker build -t postgres-backup-s3:go-migration .
go test -count=1 -tags=integration -v -timeout=15m ./tests/integration
```

The integration suite creates and removes disposable containers and a network. It checks plaintext restore, encrypted fixed-name version restore, repeated backups with a surviving GPG agent, and a multipart encrypted backup with an unwritable temporary directory. The latter restore checks row count and a checksum of all values. Tests use isolated credentials and do not contact AWS.

### Local executable

Install PostgreSQL client tools and GPG, set the runtime environment variables, then run:

```sh
go build -o /tmp/postgres-backup-s3 ./cmd/postgres-backup-s3
/tmp/postgres-backup-s3 backup
```

### Docker builds

The multistage build produces a stripped, static Go executable. The compiler and module cache stay in the build stage. The default runtime uses Alpine 3.21 and PostgreSQL client 17.

```sh
docker build -t postgres-backup-s3:local .
docker buildx build --platform linux/amd64,linux/arm64 --build-arg ALPINE_VERSION=3.21 .
```

`ALPINE_VERSION` controls the PostgreSQL client major version:

| PostgreSQL tag | Alpine version |
| --- | --- |
| `12` | `3.12` |
| `13` | `3.14` |
| `14` | `3.16` |
| `15` | `3.17` |
| `16` | `3.19` |
| `17` | `3.21` |

Older Alpine releases preserve existing PostgreSQL tags; several are outside normal support. See [Alpine's support table](https://alpinelinux.org/releases/). Alpine 3.12 and 3.14 install `gnupg`; newer images use `gpg` and `gpg-agent`.

### Development settings

| Variable / argument | Default | Scope |
| --- | --- | --- |
| `BACKUP_TEST_IMAGE` | `postgres-backup-s3:go-migration` | Environment variable for the Docker integration test. |
| `POSTGRES_TEST_IMAGE` | `postgres:17` | Environment variable for the Docker integration test's PostgreSQL image. |
| `S3_TEST_IMAGE` | MinIO image pinned by digest in `tests/integration/docker_test.go` | Environment variable for the Docker integration test's S3 fixture. |
| `ALPINE_VERSION` | `3.21` | Docker **build argument**, not a runtime environment variable. Selects the PostgreSQL client version. |
| `GO_VERSION` | `1.26` | Docker **build argument** selecting the builder's Go version. |
| `BUILDPLATFORM`, `TARGETOS`, `TARGETARCH` | Supplied by Docker BuildKit | Build-platform inputs; select target architectures with `docker buildx build --platform`. |

### Repository Compose fixture

The included [docker-compose.yaml](docker-compose.yaml) builds a PostgreSQL 14 backup image and starts a development PostgreSQL server. It still needs an S3 bucket and credentials:

```sh
cp template.env .env
# Fill in S3_REGION, S3_BUCKET and credentials in .env.
docker compose up -d --build
docker compose exec backup postgres-backup-s3 backup
```

## Compatibility

The Go implementation preserves existing runtime configuration, timestamp keys, `.dump` / `.dump.gpg` formats and shell entrypoints:

| Legacy entrypoint | Equivalent command |
| --- | --- |
| `sh run.sh` | `postgres-backup-s3 run` |
| `sh backup.sh` | `postgres-backup-s3 backup` |
| `sh restore.sh [arguments]` | `postgres-backup-s3 restore [arguments]` |

Requests use Signature Version 4; `S3_S3V4` is accepted but has no effect. `PGDUMP_EXTRA_OPTS` remains a whitespace-separated argument list, not shell code.

The early development fixed-key layout `<database>.dump` has been replaced by `<encoded-database>/latest.dump`. Old objects and VersionIds are not migrated: retrieve them using their original S3 keys. Before enabling timestamp cleanup, move old flat fixed-name backups whose names resemble timestamped dumps. Existing timestamp keys and restore arguments remain supported.

## Acknowledgements and license

This project builds on [alexanderbartels/postgres-backup-s3](https://github.com/alexanderbartels/postgres-backup-s3) and Johannes Schickling's [postgres-backup-s3](https://github.com/schickling/dockerfiles/tree/master/postgres-backup-s3) and [postgres-restore-s3](https://github.com/schickling/dockerfiles/tree/master/postgres-restore-s3).

[MIT License](LICENSE.txt).

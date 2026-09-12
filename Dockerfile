ARG ALPINE_VERSION='3.21'
ARG GO_VERSION='1.26'

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download
COPY cmd/ cmd/
COPY internal/ internal/
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags='-s -w' -o /out/postgres-backup-s3 ./cmd/postgres-backup-s3

FROM alpine:${ALPINE_VERSION}
COPY src/install.sh /install.sh
RUN sh /install.sh && rm /install.sh
COPY --from=build /out/postgres-backup-s3 /usr/local/bin/postgres-backup-s3

ENV POSTGRES_DATABASE=''
ENV POSTGRES_DATABASES=''
ENV POSTGRES_BACKUP_ALL='false'
ENV POSTGRES_DATABASES_EXCLUDE=''
ENV POSTGRES_MAINTENANCE_DB='postgres'
ENV POSTGRES_HOST=''
ENV POSTGRES_PORT=5432
ENV POSTGRES_USER=''
ENV POSTGRES_PASSWORD=''
ENV PGDUMP_EXTRA_OPTS=''
ENV S3_ACCESS_KEY_ID=''
ENV S3_SECRET_ACCESS_KEY=''
ENV S3_BUCKET=''
ENV S3_REGION='us-west-1'
ENV S3_PREFIX='backup'
ENV S3_ENDPOINT=''
ENV S3_S3V4='no'
ENV SCHEDULE=''
ENV PASSPHRASE=''
ENV BACKUP_KEEP_DAYS=''
ENV BACKUP_FILENAME_MODE='timestamp'

COPY src/run.sh src/backup.sh src/restore.sh /

CMD ["postgres-backup-s3", "run"]

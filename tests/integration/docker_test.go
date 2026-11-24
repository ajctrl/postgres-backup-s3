//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// Pin the distroless multi-platform image from the official Silo release.
// https://github.com/pgsty/silo/releases
const defaultS3TestImage = "docker.io/pgsty/silo:RELEASE.2026-10-18T00-00-00Z-distroless@sha256:472f59c7be537571f0f9a2ee6b8fe033a220939d521255bd4bf5339e53db7218"

// TestDevelopmentCompose checks interpolation without starting services or using
// any settings from the developer's environment or project .env.
func TestDevelopmentCompose(t *testing.T) {
	binary, args := "docker", []string{"compose"}
	if _, err := exec.LookPath(binary); err != nil {
		binary, args = "docker-compose", nil
	}
	args = append(args, "--env-file", os.DevNull, "-f", "../../docker-compose.yaml", "config", "--format", "json")
	for _, tc := range []struct {
		name, setting string
	}{
		{"unset", ""},
		{"empty", "PASSPHRASE="},
		{"set", "PASSPHRASE=compose-test"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.CommandContext(t.Context(), binary, args...)
			cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + t.TempDir(), "S3_SESSION_TOKEN=session-test"}
			if tc.setting != "" {
				cmd.Env = append(cmd.Env, tc.setting)
			}
			var diagnostics bytes.Buffer
			cmd.Stderr = &diagnostics
			output, err := cmd.Output()
			if tc.name != "set" {
				if err == nil || !strings.Contains(diagnostics.String(), "PASSPHRASE") {
					t.Fatalf("expected passphrase error, got %v %s", err, &diagnostics)
				}
				return
			}
			if err != nil {
				t.Fatalf("compose config: %v %s", err, &diagnostics)
			}
			var project struct {
				Services map[string]struct{ Environment map[string]string }
			}
			if err := json.Unmarshal(output, &project); err != nil {
				t.Fatal(err)
			}
			env := project.Services["backup"].Environment
			if env["PASSPHRASE"] != "compose-test" || env["S3_SESSION_TOKEN"] != "session-test" {
				t.Fatal("backup service did not receive the configured passphrase and token")
			}
		})
	}
}

// TestDockerBackupRestore requires a local Docker daemon and an already-built
// backup image. All credentials, containers and stored data belong to this test.
func TestDockerBackupRestore(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Fatal("integration tests require the docker executable")
	}
	docker(t, ctx, "info", "--format", "{{.ServerVersion}}")
	image := envOr("BACKUP_TEST_IMAGE", "postgres-backup-s3:go-migration")
	if user := strings.TrimSpace(docker(t, ctx, "image", "inspect", "--format", "{{.Config.User}}", image)); user != "65532:65532" {
		t.Fatalf("backup image runs as %q, want 65532:65532", user)
	}

	name := fmt.Sprintf("pgs3-integration-%d", time.Now().UnixNano())
	docker(t, ctx, "network", "create", name)
	t.Cleanup(func() { dockerCleanup(t, "network", "rm", name) })
	postgres := name + "-postgres"
	silo := name + "-s3"
	for _, container := range []string{postgres, silo} {
		t.Cleanup(func() { dockerCleanup(t, "rm", "-f", "-v", container) })
	}
	docker(t, ctx, "run", "-d", "--name", postgres, "--network", name, "--network-alias", "postgres",
		"-p", "127.0.0.1::9000",
		"-e", "POSTGRES_USER=integration", "-e", "POSTGRES_PASSWORD=integration-password", "-e", "POSTGRES_DB=app",
		envOr("POSTGRES_TEST_IMAGE", "postgres:18.7"))
	// Sharing one isolated network namespace also works on Docker hosts that
	// disable communication between containers on a bridge network.
	// Silo retains MINIO_* settings; its distroless entrypoint runs the server
	// directly, without a shell or bundled S3 client.
	docker(t, ctx, "run", "-d", "--name", silo, "--network", "container:"+postgres,
		"-e", "MINIO_ROOT_USER=integration", "-e", "MINIO_ROOT_PASSWORD=integration-password",
		envOr("S3_TEST_IMAGE", defaultS3TestImage), "server", "/data")

	waitFor(t, ctx, "PostgreSQL", func(ctx context.Context) error {
		_, err := exec.CommandContext(ctx, "docker", "exec", postgres, "pg_isready", "-h", "127.0.0.1", "-U", "integration", "-d", "app").CombinedOutput()
		return err
	})
	endpoint := "http://" + strings.TrimSpace(docker(t, ctx, "port", postgres, "9000/tcp"))
	client := s3.NewFromConfig(aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("integration", "integration-password", ""),
		HTTPClient:  &http.Client{Timeout: 5 * time.Second},
	}, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
		o.RetryMaxAttempts = 1
	})
	waitFor(t, ctx, "Silo", func(ctx context.Context) error {
		_, err := client.ListBuckets(ctx, &s3.ListBucketsInput{})
		return err
	})
	bucket := name
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket:                  aws.String(bucket),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
	}); err != nil {
		t.Fatal(err)
	}

	sql := func(t *testing.T, query string) string {
		t.Helper()
		return strings.TrimSpace(docker(t, ctx, "exec", postgres, "psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", "integration", "-d", "app", "-Atc", query))
	}
	sql(t, "CREATE TABLE backup_probe (id integer PRIMARY KEY, value text NOT NULL); INSERT INTO backup_probe VALUES (1, 'original');")
	assertValue := func(t *testing.T, want string) {
		t.Helper()
		if got := sql(t, "SELECT value FROM backup_probe WHERE id = 1"); got != want {
			t.Fatalf("restored value = %q, want %q", got, want)
		}
	}
	baseEnv := map[string]string{
		"PGSSLMODE": "disable", "POSTGRES_HOST": "127.0.0.1", "POSTGRES_USER": "integration", "POSTGRES_PASSWORD": "integration-password", "PGCONNECT_TIMEOUT": "10",
		"POSTGRES_DATABASE": "app", "S3_BUCKET": bucket, "S3_REGION": "us-east-1", "S3_ENDPOINT": "http://127.0.0.1:9000",
		"S3_ACCESS_KEY_ID": "integration", "S3_SECRET_ACCESS_KEY": "integration-password",
	}
	runNumber := 0
	runImage := func(t *testing.T, overrides map[string]string, command ...string) (string, error) {
		t.Helper()
		runNumber++
		container := fmt.Sprintf("%s-run-%d", name, runNumber)
		defer dockerCleanup(t, "rm", "-f", "-v", container)
		args := []string{"run", "--rm", "--name", container, "--network", "container:" + postgres}
		env := make(map[string]string, len(baseEnv)+len(overrides))
		for key, value := range baseEnv {
			env[key] = value
		}
		for key, value := range overrides {
			env[key] = value
		}
		keys := make([]string, 0, len(env))
		for key := range env {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			args = append(args, "-e", key+"="+env[key])
		}
		args = append(args, image)
		args = append(args, command...)
		out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
		return strings.TrimSpace(string(out)), err
	}
	runBackupImage := func(t *testing.T, overrides map[string]string, command ...string) {
		t.Helper()
		out, err := runImage(t, overrides, command...)
		if err != nil {
			t.Fatalf("backup image failed: %v\n%s", err, out)
		}
		t.Log(out)
	}

	if !t.Run("timestamp_plaintext", func(t *testing.T) {
		env := map[string]string{"S3_PREFIX": "timestamp//"}
		// Exercise the default CMD as well as the legacy restore wrapper.
		runBackupImage(t, env)
		objects, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String("timestamp/v2/full/app/")})
		if err != nil {
			t.Fatal(err)
		}
		if len(objects.Contents) != 1 || !strings.HasSuffix(aws.ToString(objects.Contents[0].Key), ".dump") {
			t.Fatalf("expected one timestamped dump, got %+v", objects.Contents)
		}
		_, collisionErr := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket), Key: objects.Contents[0].Key,
			Body: strings.NewReader("must not replace the backup"), IfNoneMatch: aws.String("*"),
		})
		var apiErr smithy.APIError
		if !errors.As(collisionErr, &apiErr) || apiErr.ErrorCode() != "PreconditionFailed" {
			t.Fatalf("provider did not reject generation overwrite: %v", collisionErr)
		}
		object, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: objects.Contents[0].Key})
		if err != nil {
			t.Fatal(err)
		}
		header := make([]byte, 5)
		_, readErr := io.ReadFull(object.Body, header)
		object.Body.Close()
		if readErr != nil || string(header) != "PGDMP" {
			t.Fatalf("backup is not a PostgreSQL custom-format dump: header %q, error %v", header, readErr)
		}
		sql(t, "UPDATE backup_probe SET value = 'changed after backup'")
		generation := strings.TrimSuffix(strings.TrimPrefix(aws.ToString(objects.Contents[0].Key), "timestamp/v2/full/app/"), ".dump")
		runBackupImage(t, env, "sh", "restore.sh", generation)
		assertValue(t, "original")
	}) {
		return
	}
	t.Run("partial_backup_permission", func(t *testing.T) {
		env := map[string]string{"S3_PREFIX": "partial", "PGDUMP_EXTRA_OPTS": "--table=backup_probe"}
		output, err := runImage(t, env, "postgres-backup-s3", "backup")
		if err == nil || !strings.Contains(output, "BACKUP_ALLOW_PARTIAL=true") {
			t.Fatalf("partial backup did not require explicit permission: %v\n%s", err, output)
		}
		objects, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String("partial/")})
		if err != nil || len(objects.Contents) != 0 {
			t.Fatalf("rejected partial backup published data: %v, %+v", err, objects)
		}
		env["BACKUP_ALLOW_PARTIAL"] = "true"
		runBackupImage(t, env, "postgres-backup-s3", "backup")
		objects, err = client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String("partial/")})
		if err != nil || len(objects.Contents) != 1 {
			t.Fatalf("authorized partial backup not published: %v, %+v", err, objects)
		}
	})
	t.Run("client_version_specific_option", func(t *testing.T) {
		env := map[string]string{"S3_PREFIX": "client-version", "PGDUMP_EXTRA_OPTS": "--no-statistics"}
		version, err := runImage(t, nil, "pg_dump", "--version")
		if err != nil {
			t.Fatalf("read backup-image client version: %v\n%s", err, version)
		}
		output, err := runImage(t, env, "postgres-backup-s3", "backup")
		wantObjects := 1
		if strings.Contains(version, " 17.") {
			wantObjects = 0
			if err == nil || !strings.Contains(output, "requires pg_dump 18") {
				t.Fatalf("PostgreSQL 17 did not reject version-specific option: %v\n%s", err, output)
			}
		} else if err != nil {
			t.Fatalf("supported client rejected option: %v\n%s", err, output)
		}
		objects, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String("client-version/")})
		if err != nil || len(objects.Contents) != wantObjects {
			t.Fatalf("unexpected publication after client check: %v, %+v", err, objects)
		}
	})
	t.Run("failed_restore_rolls_back", func(t *testing.T) {
		sql(t, "CREATE ROLE rollback_owner; CREATE TABLE rollback_probe (value text); ALTER TABLE rollback_probe OWNER TO rollback_owner; INSERT INTO rollback_probe VALUES ('backup');")
		env := map[string]string{"S3_PREFIX": "rollback", "BACKUP_FILENAME_MODE": "fixed"}
		runBackupImage(t, env, "postgres-backup-s3", "backup")
		sql(t, "ALTER TABLE rollback_probe OWNER TO integration; DROP ROLE rollback_owner; UPDATE rollback_probe SET value = 'before failed restore'; UPDATE backup_probe SET value = 'before failed restore';")
		output, err := runImage(t, env, "postgres-backup-s3", "restore")
		if err == nil || !strings.Contains(output, "pg_restore failed") || strings.Contains(output, "rollback_owner") || strings.Contains(output, "Restore complete") {
			t.Fatalf("expected missing-role restore failure: %v\n%s", err, output)
		}
		assertValue(t, "before failed restore")
		if value := sql(t, "SELECT value FROM rollback_probe"); value != "before failed restore" {
			t.Fatalf("failed restore changed existing data: %q", value)
		}
		env["RESTORE_NO_OWNER"] = "true"
		runBackupImage(t, env, "postgres-backup-s3", "restore")
		if value := sql(t, "SELECT value FROM rollback_probe"); value != "backup" {
			t.Fatalf("restore without original owner did not restore data: %q", value)
		}
		if owner := sql(t, "SELECT tableowner FROM pg_tables WHERE schemaname='public' AND tablename='rollback_probe'"); owner != "integration" {
			t.Fatalf("restored table owner = %q, want integration", owner)
		}
		sql(t, "DROP TABLE rollback_probe; UPDATE backup_probe SET value = 'original';")
	})
	t.Run("data_only_restore_rejected_before_sequence_changes", func(t *testing.T) {
		sql(t, "CREATE SEQUENCE a_rollback_seq; CREATE SEQUENCE z_rollback_seq; SELECT setval('a_rollback_seq', 100);")
		defer sql(t, "DROP SEQUENCE IF EXISTS a_rollback_seq, z_rollback_seq;")
		body, err := exec.CommandContext(ctx, "docker", "exec", postgres, "pg_dump", "-U", "integration", "-d", "app", "--format=custom", "--data-only").Output()
		if err != nil {
			t.Fatalf("create historical data-only archive: %v", err)
		}
		_, err = client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("data-only/v2/full/app.dump"), Body: bytes.NewReader(body)})
		if err != nil {
			t.Fatal(err)
		}
		sql(t, "SELECT setval('a_rollback_seq', 999); DROP SEQUENCE z_rollback_seq;")
		output, err := runImage(t, map[string]string{"S3_PREFIX": "data-only", "BACKUP_FILENAME_MODE": "fixed"}, "postgres-backup-s3", "restore")
		if err == nil || !strings.Contains(output, "before database access") || strings.Contains(output, "Restore complete") {
			t.Fatalf("expected archive rejection: %v\n%s", err, output)
		}
		if value := sql(t, "SELECT last_value FROM a_rollback_seq"); value != "999" {
			t.Fatalf("rejected restore changed existing sequence: %q", value)
		}
	})
	t.Run("large_object_only_archives_rejected", func(t *testing.T) {
		sql(t, "CREATE DATABASE lob_only")
		defer sql(t, "DROP DATABASE lob_only")
		lobSQL := func(query string) string {
			return strings.TrimSpace(docker(t, ctx, "exec", postgres, "psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", "integration", "-d", "lob_only", "-Atc", query))
		}
		lobSQL("SELECT lo_from_bytea(90000, convert_to('saved', 'UTF8'))")
		var archives [][]byte
		for _, extra := range [][]string{nil, {"--data-only"}} {
			args := []string{"exec", postgres, "pg_dump", "-U", "integration", "-d", "lob_only", "--format=custom"}
			body, err := exec.CommandContext(ctx, "docker", append(args, extra...)...).Output()
			if err != nil {
				t.Fatalf("create LO-only archive: %v", err)
			}
			archives = append(archives, body)
		}
		lobSQL("SELECT lo_put(90000, 0, convert_to('newer', 'UTF8'))")
		env := map[string]string{"S3_PREFIX": "lo-only", "POSTGRES_DATABASE": "lob_only", "BACKUP_FILENAME_MODE": "fixed"}
		for _, body := range archives {
			if _, err := client.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String("lo-only/v2/full/lob_only.dump"), Body: bytes.NewReader(body)}); err != nil {
				t.Fatal(err)
			}
			output, err := runImage(t, env, "postgres-backup-s3", "restore")
			if err == nil || !strings.Contains(output, "before database access") {
				t.Fatalf("LO-only restore was not rejected: %v\n%s", err, output)
			}
			if got := lobSQL("SELECT convert_from(lo_get(90000), 'UTF8')"); got != "newer" {
				t.Fatalf("rejected restore changed large-object contents: %q", got)
			}
		}
		env["S3_PREFIX"], env["BACKUP_KEEP_DAYS"] = "lo-new", "7"
		output, err := runImage(t, env, "postgres-backup-s3", "backup")
		if err == nil || !strings.Contains(output, "ambiguous archive") {
			t.Fatalf("ambiguous new backup did not fail: %v\n%s", err, output)
		}
		objects, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String("lo-new/")})
		if err != nil || len(objects.Contents) != 0 {
			t.Fatalf("ambiguous backup was published: %v, %+v", err, objects)
		}
	})
	t.Run("fixed_encrypted_versions", func(t *testing.T) {
		env := map[string]string{"S3_PREFIX": "fixed/", "BACKUP_FILENAME_MODE": "fixed", "PASSPHRASE": "integration encryption passphrase"}
		key := "fixed/v2/full/app.dump.gpg"
		version := func() string {
			t.Helper()
			object, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
			if err != nil {
				t.Fatal(err)
			}
			if aws.ToString(object.VersionId) == "" || aws.ToString(object.VersionId) == "null" || aws.ToInt64(object.ContentLength) == 0 {
				t.Fatalf("expected a nonempty versioned encrypted object, got %+v", object)
			}
			return *object.VersionId
		}
		sql(t, "UPDATE backup_probe SET value = 'first encrypted version'")
		// Run twice in one container: the GPG agent survives the first command,
		// and must not keep its inherited file descriptor locked for the next run.
		runBackupImage(t, env, "sh", "-c", "sh backup.sh && sh backup.sh")
		firstVersion := version()
		sql(t, "UPDATE backup_probe SET value = 'second encrypted version'")
		runBackupImage(t, env, "postgres-backup-s3", "backup")
		secondVersion := version()
		if firstVersion == secondVersion {
			t.Fatal("second backup did not create a new S3 version")
		}
		sql(t, "UPDATE backup_probe SET value = 'changed after both backups'")
		runBackupImage(t, env, "sh", "restore.sh", "--version-id", firstVersion)
		assertValue(t, "first encrypted version")
		runBackupImage(t, env, "postgres-backup-s3", "restore")
		assertValue(t, "second encrypted version")
	})
	t.Run("streaming_encrypted_multipart", func(t *testing.T) {
		// Incompressible enough to cross the multipart threshold even after GPG.
		sql(t, "CREATE TABLE stream_probe AS SELECT g AS id, md5(g::text) || md5((g + 1000000)::text) AS value FROM generate_series(1, 400000) AS g;")
		want := sql(t, "SELECT count(*) || ':' || md5(string_agg(value, '' ORDER BY id)) FROM stream_probe")
		env := map[string]string{
			"S3_PREFIX": "streaming", "BACKUP_FILENAME_MODE": "timestamp", "PASSPHRASE": "streaming passphrase",
			"S3_UPLOAD_PART_SIZE_MB": "5", "PGDUMP_EXTRA_OPTS": "--compress=0", "TMPDIR": "/proc",
		}
		// /proc cannot hold temporary dump files. Only streaming can succeed here.
		runBackupImage(t, env, "postgres-backup-s3", "backup")
		objects, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String("streaming/v2/full/app/")})
		if err != nil || len(objects.Contents) != 1 {
			t.Fatalf("expected one multipart generation: %v, %v", objects, err)
		}
		object, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: objects.Contents[0].Key})
		if err != nil {
			t.Fatal(err)
		}
		if aws.ToInt64(object.ContentLength) <= 5*1024*1024 || !strings.Contains(aws.ToString(object.ETag), "-") {
			t.Fatalf("expected an encrypted multipart object: size=%d etag=%s", aws.ToInt64(object.ContentLength), aws.ToString(object.ETag))
		}
		sql(t, "TRUNCATE stream_probe")
		// Restore still validates downloaded/decrypted files before touching the DB.
		env["TMPDIR"] = "/tmp"
		runBackupImage(t, env, "postgres-backup-s3", "restore")
		if got := sql(t, "SELECT count(*) || ':' || md5(string_agg(value, '' ORDER BY id)) FROM stream_probe"); got != want {
			t.Fatalf("streaming round trip changed data: got %q, want %q", got, want)
		}
	})
}

func docker(t *testing.T, ctx context.Context, args ...string) string {
	t.Helper()
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %s failed: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

func dockerCleanup(t *testing.T, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil && !strings.Contains(string(out), "No such container") && !strings.Contains(string(out), "not found") {
		t.Logf("cleanup docker %s: %v\n%s", strings.Join(args, " "), err, out)
	}
}

func waitFor(t *testing.T, ctx context.Context, service string, probe func(context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	var lastErr error
	for {
		probeCtx, probeCancel := context.WithTimeout(ctx, 5*time.Second)
		lastErr = probe(probeCtx)
		probeCancel()
		if lastErr == nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for %s: %v (last probe: %v)", service, ctx.Err(), lastErr)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

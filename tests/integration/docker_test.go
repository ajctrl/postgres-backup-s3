//go:build integration

package integration_test

import (
	"context"
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
)

const defaultS3TestImage = "quay.io/minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"

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
	docker(t, ctx, "image", "inspect", image)

	name := fmt.Sprintf("pgs3-integration-%d", time.Now().UnixNano())
	docker(t, ctx, "network", "create", name)
	t.Cleanup(func() { dockerCleanup(t, "network", "rm", name) })
	postgres := name + "-postgres"
	minio := name + "-s3"
	for _, container := range []string{postgres, minio} {
		t.Cleanup(func() { dockerCleanup(t, "rm", "-f", "-v", container) })
	}
	docker(t, ctx, "run", "-d", "--name", postgres, "--network", name, "--network-alias", "postgres",
		"-p", "127.0.0.1::9000",
		"-e", "POSTGRES_USER=integration", "-e", "POSTGRES_PASSWORD=integration-password", "-e", "POSTGRES_DB=app",
		envOr("POSTGRES_TEST_IMAGE", "postgres:17"))
	// Sharing one isolated network namespace also works on Docker hosts that
	// disable communication between containers on a bridge network.
	docker(t, ctx, "run", "-d", "--name", minio, "--network", "container:"+postgres,
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
	waitFor(t, ctx, "MinIO", func(ctx context.Context) error {
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
		"POSTGRES_HOST": "127.0.0.1", "POSTGRES_USER": "integration", "POSTGRES_PASSWORD": "integration-password", "PGCONNECT_TIMEOUT": "10",
		"POSTGRES_DATABASE": "app", "S3_BUCKET": bucket, "S3_REGION": "us-east-1", "S3_ENDPOINT": "http://127.0.0.1:9000",
		"S3_ACCESS_KEY_ID": "integration", "S3_SECRET_ACCESS_KEY": "integration-password",
	}
	runNumber := 0
	runBackupImage := func(t *testing.T, overrides map[string]string, command ...string) {
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
		t.Log(strings.TrimSpace(docker(t, ctx, args...)))
	}

	if !t.Run("timestamp_plaintext", func(t *testing.T) {
		// MinIO rejects empty key path components; legacy repeated-slash S3 keys
		// are covered by the unit/HTTP tests instead of this provider fixture.
		env := map[string]string{"S3_PREFIX": "timestamp"}
		// Exercise the default CMD as well as the legacy restore wrapper.
		runBackupImage(t, env)
		objects, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String("timestamp/app/")})
		if err != nil {
			t.Fatal(err)
		}
		if len(objects.Contents) != 1 || !strings.HasSuffix(aws.ToString(objects.Contents[0].Key), ".dump") {
			t.Fatalf("expected one timestamped dump, got %+v", objects.Contents)
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
		runBackupImage(t, env, "sh", "restore.sh")
		assertValue(t, "original")
	}) {
		return
	}
	t.Run("fixed_encrypted_versions", func(t *testing.T) {
		env := map[string]string{"S3_PREFIX": "fixed/", "BACKUP_FILENAME_MODE": "fixed", "PASSPHRASE": "integration encryption passphrase"}
		key := "fixed/app.dump.gpg"
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
			"S3_PREFIX": "streaming", "BACKUP_FILENAME_MODE": "fixed", "PASSPHRASE": "streaming passphrase",
			"S3_UPLOAD_PART_SIZE_MB": "5", "PGDUMP_EXTRA_OPTS": "--compress=0", "TMPDIR": "/proc",
		}
		// /proc cannot hold temporary dump files. Only streaming can succeed here.
		runBackupImage(t, env, "postgres-backup-s3", "backup")
		object, err := client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String("streaming/app.dump.gpg")})
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

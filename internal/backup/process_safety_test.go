package backup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestChildCredentials(t *testing.T) {
	for _, key := range []string{"POSTGRES_PASSWORD", "S3_SECRET_ACCESS_KEY", "AWS_ACCESS_KEY_ID", "AWS_SESSION_TOKEN", "PGPASSWORD", "PASSPHRASE", "PGPASSFILE"} {
		t.Setenv(key, "fixture")
	}
	for _, name := range []string{"gpg", "psql", "pg_dump", "pg_restore"} {
		cmd := (commandRunner{password: "selected"}).command(t.Context(), name)
		values := map[string]string{}
		for _, v := range cmd.Env {
			k, value, _ := strings.Cut(v, "=")
			values[k] = value
		}
		for _, key := range []string{"POSTGRES_PASSWORD", "S3_SECRET_ACCESS_KEY", "AWS_ACCESS_KEY_ID", "AWS_SESSION_TOKEN", "PASSPHRASE"} {
			if _, found := values[key]; found {
				t.Errorf("%s inherited %s", name, key)
			}
		}
		if name == "gpg" {
			for _, key := range []string{"PGPASSWORD", "PGPASSFILE"} {
				if _, found := values[key]; found {
					t.Errorf("gpg inherited %s", key)
				}
			}
		} else if values["PGPASSWORD"] != "selected" || values["PGPASSFILE"] != "fixture" {
			t.Errorf("%s lost libpq configuration", name)
		}
	}
}

func TestPostgresTLSDefaultAndOverride(t *testing.T) {
	for _, mode := range []string{"", "require", "disable"} {
		t.Setenv("PGSSLMODE", mode)
		want := mode
		if want == "" {
			want = "verify-full"
		}
		app := New(Config{}, nil, nil, nil)
		if got := app.connectionArgs("app")[1]; got != "postgresql:///app?sslmode="+want {
			t.Fatal(got)
		}
		if got := app.dumpConnectionArgs("app", false)[1]; got != "postgresql:///app?sslmode="+want+"&target_session_attrs=primary" {
			t.Fatal(got)
		}
	}
}

func TestPipelineConsumerReceivesEOFBeforeProducerExits(t *testing.T) {
	for _, exitCode := range []int{0, 1} {
		t.Run(fmt.Sprint(exitCode), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			dir := t.TempDir()
			archive := append(customArchiveFixture(16, tocFixture{kind: "DATABASE", section: 2, definition: "CREATE DATABASE app"}), []byte("payload")...)
			path := filepath.Join(dir, "dump")
			if err := os.WriteFile(path, archive, 0600); err != nil {
				t.Fatal(err)
			}
			consumerDone, release := filepath.Join(dir, "consumer-done"), filepath.Join(dir, "release")
			var out bytes.Buffer
			done := make(chan error, 1)
			go func() {
				done <- (commandRunner{stderr: io.Discard}).pipeline(ctx, &out,
					pipelineCommand{name: "sh", customDump: true, args: []string{"-c", `cat "$3"; exec 1>&-; while [ ! -f "$1" ]; do sleep 0.01; done; exit "$2"`, "producer", release, fmt.Sprint(exitCode), path}},
					pipelineCommand{name: "sh", args: []string{"-c", `cat; touch "$1"`, "consumer", consumerDone}})
			}()
			// Always release/cancel and join the pipeline, including assertion failures.
			defer func() { cancel(); <-done }()
			for {
				if _, err := os.Stat(consumerDone); err == nil {
					break
				}
				select {
				case err := <-done:
					done <- err
					t.Fatalf("pipeline stopped before consumer EOF: %v", err)
				case <-ctx.Done():
					t.Fatal("consumer never received EOF while producer was alive")
				case <-time.After(10 * time.Millisecond):
				}
			}
			select {
			case err := <-done:
				done <- err
				t.Fatalf("pipeline completed before producer exit: %v", err)
			default:
			}
			if err := os.WriteFile(release, nil, 0600); err != nil {
				t.Fatal(err)
			}
			err := <-done
			done <- err
			if (err == nil) != (exitCode == 0) || errors.Is(err, context.DeadlineExceeded) || !bytes.Equal(out.Bytes(), archive) {
				t.Fatalf("producer exit %d: error=%v, output=%q", exitCode, err, &out)
			}
		})
	}
}

func TestCustomDumpValidationBeforeConsumer(t *testing.T) {
	dir := t.TempDir()
	// Force parser read-ahead and multiple subsequent copies, checking that the
	// tee-based validation neither loses nor duplicates any stream bytes.
	archive := append(customArchiveFixture(16, tocFixture{kind: "DATABASE", section: 2, definition: "CREATE DATABASE app"}), bytes.Repeat([]byte{0, 1, 2, 255}, 32768)...)
	path := filepath.Join(dir, "dump")
	if err := os.WriteFile(path, archive, 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ script, want string }{
		{`head -c 2 "$1"; tail -c +3 "$1"`, string(archive)},
		{"true", ""}, {"printf PGDM", ""}, {"printf 'SQL data'", ""},
	} {
		t.Run(tc.script, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			var out bytes.Buffer
			err := (commandRunner{stderr: io.Discard}).pipeline(ctx, &out,
				pipelineCommand{name: "sh", args: []string{"-c", tc.script, "producer", path}, customDump: true},
				pipelineCommand{name: "cat"})
			if tc.want != "" && (err != nil || out.String() != tc.want) {
				t.Fatalf("valid stream failed: %v, %q", err, &out)
			}
			if tc.want == "" && (!errors.Is(err, errInvalidDump) || out.Len() != 0) {
				t.Fatalf("invalid stream accepted/forwarded: %v, %q", err, &out)
			}
		})
	}
}

func TestDumpClientVersionGate(t *testing.T) {
	dir := t.TempDir()
	tool := filepath.Join(dir, "pg_dump")
	if err := os.WriteFile(tool, []byte("#!/bin/sh\n[ \"$#\" = 1 ] && [ \"$1\" = --version ] || exit 7\nprintf '%s\\n' \"$TEST_PG_VERSION\"\nexit \"${TEST_PG_EXIT:-0}\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	for _, tc := range []struct {
		version string
		valid   bool
	}{
		{"pg_dump (PostgreSQL) 17.11", false},
		{"pg_dump (PostgreSQL) 18.0", true},
		{"pg_dump (PostgreSQL) 18.2 (vendor build)", true},
		{"pg_dump (PostgreSQL) unknown", false}, {"unrecognized", false}, {"mariadb 18.0", false},
	} {
		t.Setenv("TEST_PG_VERSION", tc.version)
		if err := (commandRunner{stderr: io.Discard}).checkDumpClient(t.Context(), 18); (err == nil) != tc.valid {
			t.Errorf("version %q: %v", tc.version, err)
		}
		if !tc.valid {
			// A nil store makes any accidental S3 access before rejection fail.
			app := New(Config{Database: "app", BackupAll: "false", FilenameMode: "fixed", DumpOptions: "--no-statistics"}, nil, io.Discard, io.Discard)
			app.lockPath = filepath.Join(dir, "backup.lock")
			if err := app.Backup(t.Context()); err == nil || !strings.Contains(err.Error(), "PGDUMP_EXTRA_OPTS") {
				t.Fatalf("backup did not reject incompatible client before storage: %v", err)
			}
		}
	}
	t.Setenv("TEST_PG_VERSION", "pg_dump (PostgreSQL) 18.0")
	t.Setenv("TEST_PG_EXIT", "1")
	if err := (commandRunner{stderr: io.Discard}).checkDumpClient(t.Context(), 18); err == nil {
		t.Fatal("failed version command was accepted")
	}
	if err := (commandRunner{stderr: io.Discard}).checkDumpClient(t.Context(), 0); err != nil {
		t.Fatal("ordinary options unexpectedly required a version probe")
	}
}

func TestPassphrasePipeWithAndWithoutInheritedLock(t *testing.T) {
	for _, withLock := range []bool{false, true} {
		for _, secret := range []string{" spaces \\ ' \" $ end ", strings.Repeat("s", 128*1024)} {
			dir := t.TempDir()
			tool := filepath.Join(dir, "fake-gpg")
			script := `#!/bin/sh
set -eu
[ -z "${PASSPHRASE+x}" ] || exit 40
[ "$1" = --passphrase-fd ] || exit 41
case "$2" in
  3) IFS= read -r secret <&3;;
  4) IFS= read -r secret <&4;;
  *) exit 42;;
esac
[ "$#" = 2 ] || exit 43
printf '%s' "$secret"
`
			if err := os.WriteFile(tool, []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PASSPHRASE", "must not be inherited")
			runner := commandRunner{stderr: io.Discard}
			if withLock {
				lock, err := acquireLock(filepath.Join(dir, "lock"))
				if err != nil {
					t.Fatal(err)
				}
				defer lock.Close()
				runner.lock = lock
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			var out bytes.Buffer
			err := runner.pipeline(ctx, &out, pipelineCommand{name: tool, passphrase: secret})
			cancel()
			if err != nil || out.String() != secret {
				t.Fatalf("passphrase transfer failed with lock=%v: %v", withLock, err)
			}
		}
	}
}

func TestPassphrasePipeUnblocksOnChildFailure(t *testing.T) {
	dir := t.TempDir()
	tool := filepath.Join(dir, "no-read")
	if err := os.WriteFile(tool, []byte("#!/bin/sh\nexit 1\n"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{tool, filepath.Join(dir, "missing")} {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		err := (commandRunner{stderr: io.Discard}).pipeline(ctx, io.Discard,
			pipelineCommand{name: name, passphrase: strings.Repeat("s", 128*1024)})
		cancel()
		if err == nil || errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("failed child did not clean up promptly: %v", err)
		}
	}
}

func TestPassphraseRejectsLineTruncation(t *testing.T) {
	for _, secret := range []string{"a\nb", "a\rb", "a\x00b"} {
		if _, err := newPassphrasePipe(secret); err == nil {
			t.Fatal("accepted a passphrase that would be truncated")
		}
	}
}

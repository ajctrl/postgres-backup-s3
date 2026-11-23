package backup

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bartels/postgres-backup-s3/internal/storage"
)

type capturedBackupStore struct {
	publishingStore
	archive bytes.Buffer
}

func (s *capturedBackupStore) Upload(ctx context.Context, key string, body io.ReadCloser, options storage.UploadOptions) error {
	defer body.Close()
	return s.publishingStore.Upload(ctx, key, io.NopCloser(io.TeeReader(body, &s.archive)), options)
}

// Reuse the private UTF-8 cluster created by TestRestorePlacementWithPostgreSQL.
func testBackupSafetyWithPostgreSQL(t *testing.T, dir, bin string, connection []string,
	run func(string, ...string) []byte, sql func(string, string) string) {
	t.Helper()
	t.Run("subscriptions-use-effective-dump-role", func(t *testing.T) {
		sql("postgres", "CREATE ROLE subscription_reader LOGIN")
		defer sql("postgres", "DROP ROLE subscription_reader")
		sql("postgres", "GRANT pg_read_all_data TO subscription_reader")
		sql("postgres", "CREATE DATABASE subscription_source")
		defer sql("postgres", "DROP DATABASE subscription_source")
		sql("subscription_source", "CREATE TABLE probe(id integer)")
		sql("subscription_source", "CREATE SUBSCRIPTION sub_probe CONNECTION 'dbname=postgres' PUBLICATION pub_probe WITH (connect=false, slot_name=NONE)")
		defer sql("subscription_source", "DROP SUBSCRIPTION sub_probe")
		t.Setenv("PG_COLOR", "always")
		for _, tc := range []struct {
			user, options                           string
			allowPartial, success, wantSubscription bool
		}{
			{"subscription_reader", "", false, false, false},
			{"restore_test", "--role=subscription_reader", false, false, false},
			{"restore_test", "--role subscription_reader", true, true, false},
			{"restore_test", "", false, true, true},
		} {
			var log bytes.Buffer
			s := &capturedBackupStore{}
			a := New(Config{Database: "subscription_source", BackupAll: "false", Host: dir, Port: "55440",
				User: tc.user, DumpOptions: tc.options, AllowPartial: tc.allowPartial, FilenameMode: "timestamp", KeepDays: "7"}, s, &log, &log)
			a.lockPath = filepath.Join(dir, "backup.lock")
			err := a.Backup(t.Context())
			if (err == nil) != tc.success || (s.published == 1) != tc.success || (s.prefix != "") != tc.success {
				t.Fatalf("user=%s opts=%q partial=%v: err=%v published=%d listed=%q\n%s", tc.user, tc.options, tc.allowPartial, err, s.published, s.prefix, &log)
			}
			if !tc.success {
				if !strings.Contains(log.String(), "pg_dump omitted subscriptions") {
					t.Fatalf("missing omission diagnostic: %s", &log)
				}
				continue
			}
			path := filepath.Join(t.TempDir(), "subscriptions.dump")
			if err := os.WriteFile(path, s.archive.Bytes(), 0600); err != nil {
				t.Fatal(err)
			}
			toc := string(run("pg_restore", "--list", path))
			if strings.Contains(toc, " SUBSCRIPTION - sub_probe ") != tc.wantSubscription {
				t.Fatalf("unexpected subscription in archive for user %s options %q:\n%s", tc.user, tc.options, toc)
			}
		}
	})

	t.Run("lock-timeout-continues-with-next-database", func(t *testing.T) {
		sql("postgres", "CREATE DATABASE locked_source")
		defer sql("postgres", "DROP DATABASE locked_source")
		sql("locked_source", "CREATE TABLE probe(id integer)")
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		holder := exec.CommandContext(ctx, filepath.Join(bin, "psql"), append(append([]string{}, connection...), "-XqAt", "-v", "ON_ERROR_STOP=1", "-d", "locked_source")...)
		in, err := holder.StdinPipe()
		if err != nil {
			t.Fatal(err)
		}
		out, err := holder.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		holder.Stderr = os.Stderr
		if err := holder.Start(); err != nil {
			t.Fatal(err)
		}
		defer func() {
			in.Close()
			holder.Wait()
		}()
		if _, err := io.WriteString(in, "BEGIN; LOCK TABLE probe IN ACCESS EXCLUSIVE MODE; SELECT 'locked';\n"); err != nil {
			t.Fatal(err)
		}
		if line, err := bufio.NewReader(out).ReadString('\n'); err != nil || line != "locked\n" {
			t.Fatalf("lock acquisition: %q, %v", line, err)
		}
		s := &publishingStore{}
		var log bytes.Buffer
		a := New(Config{Databases: "locked_source,postgres", BackupAll: "false", Host: dir, Port: "55440", User: "restore_test",
			DatabaseTimeout: 2 * time.Second, FilenameMode: "timestamp", KeepDays: "7"}, s, &log, &log)
		a.lockPath = filepath.Join(dir, "backup.lock")
		if err := a.Backup(ctx); err == nil || s.published != 1 || s.prefix != a.Config.timestampPrefix("postgres") || !strings.Contains(log.String(), "deadline exceeded") {
			t.Fatalf("backup after lock timeout: %v, published=%d prefix=%q\n%s", err, s.published, s.prefix, &log)
		}
		log.Reset()
		a.Config.DumpOptions = "--lock-wait-timeout=200ms"
		a.Config.DatabaseTimeout = 10 * time.Second
		if err := a.Backup(ctx); err == nil || s.published != 2 || strings.Contains(log.String(), "deadline exceeded") || !strings.Contains(log.String(), "timeout") {
			t.Fatalf("pg_dump lock timeout override: %v, published=%d\n%s", err, s.published, &log)
		}
		a.Config.Databases = "postgres"
		if err := a.Backup(ctx); err != nil || s.published != 3 {
			t.Fatalf("next backup could not complete: %v\n%s", err, &log)
		}
	})

	t.Run("discovery-overrides-latin1", func(t *testing.T) {
		sql("postgres", `CREATE DATABASE "café"`)
		defer sql("postgres", `DROP DATABASE "café"`)
		t.Setenv("PGCLIENTENCODING", "LATIN1")
		a := New(Config{BackupAll: "true", MaintenanceDB: "postgres", Host: dir, Port: "55440", User: "restore_test"}, nil, io.Discard, io.Discard)
		names, err := a.databases(t.Context(), commandRunner{stderr: io.Discard})
		if err != nil || !slices.Contains(names, "café") {
			t.Fatalf("UTF-8 name not preserved: %q %v", names, err)
		}
		// Restore the encoding before the deferred SQL cleanup.
		t.Setenv("PGCLIENTENCODING", "UTF8")
	})

	t.Run("restore-hides-user-mapping-password", func(t *testing.T) {
		sql("postgres", "CREATE DATABASE mapping_source")
		sql("postgres", "CREATE DATABASE mapping_target")
		defer sql("postgres", "DROP DATABASE mapping_source")
		defer sql("postgres", "DROP DATABASE mapping_target")
		sql("postgres", "CREATE ROLE mapping_owner")
		sql("mapping_source", `CREATE EXTENSION postgres_fdw;
CREATE TABLE probe(v integer); INSERT INTO probe VALUES (1);
CREATE SERVER mapping_server FOREIGN DATA WRAPPER postgres_fdw;
CREATE USER MAPPING FOR mapping_owner SERVER mapping_server OPTIONS (password 'synthetic-review-secret');`)
		archive := run("pg_dump", append(append([]string{}, connection...), "-d", "mapping_source", "--format=custom")...)
		sql("mapping_source", "DROP USER MAPPING FOR mapping_owner SERVER mapping_server")
		sql("postgres", "DROP ROLE mapping_owner")
		sql("mapping_target", "CREATE TABLE probe(v integer); INSERT INTO probe VALUES (99)")
		// Establish that this actual pg_restore failure would leak the secret.
		args := append(append([]string{}, connection...), "-d", "mapping_target", "--single-transaction", "--exit-on-error", "--clean", "--if-exists")
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, filepath.Join(bin, "pg_restore"), args...)
		command.Stdin = bytes.NewReader(archive)
		raw, err := command.CombinedOutput()
		if err == nil || !bytes.Contains(raw, []byte("synthetic-review-secret")) {
			t.Fatal("secret-leaking pg_restore failure was not reproduced")
		}
		var log bytes.Buffer
		a := New(Config{Database: "mapping_target", Host: dir, Port: "55440", User: "restore_test", FilenameMode: "fixed"},
			&archiveDownloadStore{archive: archive}, &log, &log)
		a.lockPath = filepath.Join(dir, "backup.lock")
		err = a.Restore(ctx, nil)
		if err == nil || !strings.Contains(err.Error(), "pg_restore failed") {
			t.Fatalf("expected safe restore failure: %v", err)
		}
		for _, secret := range []string{"synthetic-review-secret", "CREATE USER MAPPING", "mapping_owner"} {
			if strings.Contains(log.String()+err.Error(), secret) {
				t.Fatal("restore diagnostics exposed SQL or secrets")
			}
		}
		if got := sql("mapping_target", "SELECT v FROM probe"); got != "99" {
			t.Fatalf("failed restore changed data: %s", got)
		}
	})

	t.Run("standby-unlogged-policy", func(t *testing.T) {
		sql("postgres", "CREATE DATABASE unlogged_source")
		sql("postgres", "CREATE DATABASE logged_source")
		sql("unlogged_source", `CREATE TABLE logged_probe(v integer); INSERT INTO logged_probe VALUES (1);
CREATE UNLOGGED TABLE unlogged_probe(v integer); INSERT INTO unlogged_probe VALUES (2);
CREATE UNLOGGED SEQUENCE unlogged_sequence; SELECT nextval('unlogged_sequence');`)
		sql("logged_source", "CREATE TABLE logged_probe(v integer); INSERT INTO logged_probe VALUES (1)")
		standby := filepath.Join(dir, "standby")
		run("pg_basebackup", append(append([]string{}, connection...), "-D", standby, "-X", "stream", "-R", "-c", "fast")...)
		opts := "-k '" + strings.ReplaceAll(dir, "'", "'\\''") + "' -p 55441 -c listen_addresses=''"
		run("pg_ctl", "-D", standby, "-l", filepath.Join(dir, "standby.log"), "-o", opts, "-w", "start")
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if output, err := exec.CommandContext(ctx, filepath.Join(bin, "pg_ctl"), "-D", standby, "-m", "immediate", "-w", "stop").CombinedOutput(); err != nil {
				t.Errorf("stop temporary standby: %v\n%s", err, output)
			}
		})
		sequenceArchive := run("pg_dump", "-h", dir, "-p", "55441", "-U", "restore_test", "-d", "unlogged_source", "--format=custom", "--table=public.unlogged_sequence")
		if err := checkArchive(t.Context(), bytes.NewReader(sequenceArchive), true); err == nil || !strings.Contains(err.Error(), "UNLOGGED") {
			t.Fatal("streaming guard failed to identify an actual UNLOGGED sequence")
		}
		testDumpConnectionRoles(t, dir, bin, sql)
		for _, tc := range []struct {
			name, db, port   string
			partial, success bool
			unloggedRows     string
		}{
			{"primary-unlogged", "unlogged_source", "55440", false, true, "1"},
			{"standby-default", "unlogged_source", "55441", false, false, ""},
			{"standby-opt-in", "unlogged_source", "55441", true, true, "0"},
			{"standby-logged-only", "logged_source", "55441", false, true, ""},
		} {
			t.Run(tc.name, func(t *testing.T) {
				store := &capturedBackupStore{}
				var log bytes.Buffer
				a := New(Config{Database: tc.db, BackupAll: "false", Host: dir, Port: tc.port, User: "restore_test",
					FilenameMode: "timestamp", KeepDays: "7", AllowPartial: tc.partial}, store, &log, &log)
				a.lockPath = filepath.Join(dir, "backup.lock")
				ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
				defer cancel()
				err := a.Backup(ctx)
				if (err == nil) != tc.success {
					t.Fatalf("backup: %v\n%s", err, &log)
				}
				if !tc.success {
					if store.uploads != 0 || store.prefix != "" || !strings.Contains(log.String(), "UNLOGGED") {
						t.Fatalf("standby rejection did not precede upload and retention: %s", &log)
					}
					return
				}
				if store.published != 1 || store.prefix == "" {
					t.Fatal("successful backup did not publish and run retention")
				}
				if tc.partial && !strings.Contains(log.String(), "WARNING: standby backup") {
					t.Fatal("missing omission warning")
				}
				sql("postgres", "CREATE DATABASE standby_restored")
				defer sql("postgres", "DROP DATABASE standby_restored")
				restore := New(Config{Database: "standby_restored", Host: dir, Port: "55440", User: "restore_test", FilenameMode: "fixed"},
					&archiveDownloadStore{archive: store.archive.Bytes()}, &log, &log)
				restore.lockPath = a.lockPath
				if err := restore.Restore(ctx, nil); err != nil {
					t.Fatalf("restore: %v\n%s", err, &log)
				}
				if got := sql("standby_restored", "SELECT count(*) FROM logged_probe"); got != "1" {
					t.Fatalf("logged data lost: %s", got)
				}
				if tc.unloggedRows != "" {
					if got := sql("standby_restored", "SELECT count(*) FROM unlogged_probe"); got != tc.unloggedRows {
						t.Fatalf("unexpected UNLOGGED contents: %s, want %s", got, tc.unloggedRows)
					}
					if err := checkArchive(ctx, bytes.NewReader(store.archive.Bytes()), true); err == nil || !strings.Contains(err.Error(), "UNLOGGED") {
						t.Fatal("streaming guard failed to identify actual UNLOGGED definitions")
					}
				}
			})
		}
	})
}

func testDumpConnectionRoles(t *testing.T, dir, bin string, sql func(string, string) string) {
	t.Helper()
	for _, tc := range []struct {
		name, preflightPort, dumpPorts string
		partial, success               bool
		wantRows                       string
	}{
		{"primary-skips-standby", "55440", "55441,55440", false, true, "1"},
		{"primary-unavailable", "55440", "55441", false, false, ""},
		{"standby-skips-primary", "55441", "55440,55441", true, true, "0"},
		{"standby-unavailable", "55441", "55440", true, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Deterministically route the real preflight query to one server,
			// while pg_dump encounters the other role first. This models a
			// load-balancer/failover without a probabilistic random-host test.
			wrapper := t.TempDir()
			psql := strings.ReplaceAll(filepath.Join(bin, "psql"), "'", "'\\''")
			script := "#!/bin/sh\nexec '" + psql + "' \"$@\" -p " + tc.preflightPort + "\n"
			if err := os.WriteFile(filepath.Join(wrapper, "psql"), []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", wrapper+string(os.PathListSeparator)+os.Getenv("PATH"))
			t.Setenv("PGTARGETSESSIONATTRS", "any")
			t.Setenv("PGLOADBALANCEHOSTS", "disable")
			host := dir
			if strings.Contains(tc.dumpPorts, ",") {
				host += "," + dir
			}
			store := &capturedBackupStore{}
			var log bytes.Buffer
			a := New(Config{Database: "unlogged_source", BackupAll: "false", Host: host, Port: tc.dumpPorts, User: "restore_test",
				FilenameMode: "timestamp", KeepDays: "7", AllowPartial: tc.partial}, store, &log, &log)
			a.lockPath = filepath.Join(dir, "backup.lock")
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			err := a.Backup(ctx)
			if (err == nil) != tc.success {
				t.Fatalf("backup result: %v\n%s", err, &log)
			}
			if !tc.success {
				if store.published != 0 || store.prefix != "" {
					t.Fatal("unavailable source role published data or ran retention")
				}
				return
			}
			sql("postgres", "CREATE DATABASE role_restored")
			defer sql("postgres", "DROP DATABASE role_restored")
			restore := New(Config{Database: "role_restored", Host: dir, Port: "55440", User: "restore_test", FilenameMode: "fixed"},
				&archiveDownloadStore{archive: store.archive.Bytes()}, &log, &log)
			restore.lockPath = a.lockPath
			if err := restore.Restore(ctx, nil); err != nil {
				t.Fatalf("restore: %v\n%s", err, &log)
			}
			if got := sql("role_restored", "SELECT count(*) FROM unlogged_probe"); got != tc.wantRows {
				t.Fatalf("dump used the wrong role: UNLOGGED rows=%s want=%s", got, tc.wantRows)
			}
		})
	}
}

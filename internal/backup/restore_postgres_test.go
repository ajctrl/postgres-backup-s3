package backup

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type archiveDownloadStore struct {
	recordingStore
	archive []byte
}

func (s *archiveDownloadStore) Download(_ context.Context, _, _, path string) error {
	return os.WriteFile(path, s.archive, 0600)
}

// Exercise configuration, archive validation, and the actual restore command
// against PostgreSQL. S3 download is supplied by an in-memory test store.
func TestRestorePlacementWithPostgreSQL(t *testing.T) {
	bin := postgresTestBin(t)
	dir, err := os.MkdirTemp("", "pgs3-restore-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	run := func(tool string, args ...string) []byte {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		output, err := exec.CommandContext(ctx, filepath.Join(bin, tool), args...).CombinedOutput()
		if err != nil {
			t.Fatalf("%s: %v\n%s", tool, err, output)
		}
		return output
	}
	cluster := filepath.Join(dir, "cluster")
	run("initdb", "-D", cluster, "-U", "restore_test", "-A", "trust", "--no-locale", "--encoding=UTF8")
	opts := "-k '" + strings.ReplaceAll(dir, "'", "'\\''") + "' -p 55440 -c listen_addresses=''"
	run("pg_ctl", "-D", cluster, "-l", filepath.Join(dir, "postgres.log"), "-o", opts, "-w", "start")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if output, err := exec.CommandContext(ctx, filepath.Join(bin, "pg_ctl"), "-D", cluster, "-m", "immediate", "-w", "stop").CombinedOutput(); err != nil {
			t.Errorf("stop temporary PostgreSQL: %v\n%s", err, output)
		}
	})
	connection := []string{"-h", dir, "-p", "55440", "-U", "restore_test"}
	sql := func(db, query string) string {
		return strings.TrimSpace(string(run("psql", append(append([]string{}, connection...), "-d", db, "-X", "-At", "-v", "ON_ERROR_STOP=1", "-c", query)...)))
	}
	testBackupSafetyWithPostgreSQL(t, dir, bin, connection, run, sql)
	testMetadataQueriesWithPostgreSQL(t, dir, bin)
	t.Run("reject-ambiguous-large-object-only", func(t *testing.T) {
		sql("postgres", "CREATE DATABASE lob_probe")
		defer sql("postgres", "DROP DATABASE lob_probe")
		sql("lob_probe", "SELECT lo_from_bytea(90000, convert_to('saved', 'UTF8'))")
		var archives [][]byte
		for _, extra := range [][]string{nil, {"--data-only"}} {
			args := append(append([]string{}, connection...), "-d", "lob_probe", "--format=custom")
			archives = append(archives, run("pg_dump", append(args, extra...)...))
		}
		sql("lob_probe", "SELECT lo_put(90000, 0, convert_to('newer', 'UTF8'))")
		for _, archive := range archives {
			app := New(Config{Database: "lob_probe", Host: dir, Port: "55440", User: "restore_test", FilenameMode: "fixed"},
				&archiveDownloadStore{archive: archive}, io.Discard, io.Discard)
			app.lockPath = filepath.Join(dir, "backup.lock")
			if err := app.Restore(t.Context(), nil); err == nil || !strings.Contains(err.Error(), "before database access") {
				t.Fatalf("ambiguous LO archive was not rejected before restore: %v", err)
			}
			if got := sql("lob_probe", "SELECT convert_from(lo_get(90000), 'UTF8')"); got != "newer" {
				t.Fatalf("rejected archive changed an existing large object: %s", got)
			}
		}
	})
	space := filepath.Join(dir, "tablespace")
	if err := os.Mkdir(space, 0700); err != nil {
		t.Fatal(err)
	}
	sql("postgres", "CREATE TABLESPACE restore_space LOCATION '"+strings.ReplaceAll(space, "'", "''")+"'")
	sql("postgres", "CREATE DATABASE source")
	sql("source", `CREATE ACCESS METHOD restore_heap TYPE TABLE HANDLER heap_tableam_handler;
CREATE TABLE probe(v double precision) USING restore_heap TABLESPACE restore_space;
INSERT INTO probe VALUES (1.2345678901234567);`)
	wantBits := sql("source", "SELECT encode(float8send(v), 'hex') FROM probe")
	archive := run("pg_dump", append(append([]string{}, connection...), "-d", "source", "--format=custom")...)
	sql("postgres", "DROP DATABASE source")
	sql("postgres", "DROP TABLESPACE restore_space")
	for _, tc := range []struct {
		name, noTablespaces, noAccessMethod, wantMethod string
	}{
		{"defaults", "", "", ""},
		{"explicit-false", "false", "false", ""},
		{"access-method-only", "false", "true", ""},
		{"tablespaces-only", "true", "false", "restore_heap"},
		{"both", "true", "true", "heap"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql("postgres", "CREATE DATABASE restored")
			defer sql("postgres", "DROP DATABASE restored")
			sql("restored", "CREATE TABLE probe(v double precision); INSERT INTO probe VALUES (99)")
			env := map[string]string{
				"S3_BUCKET": "test", "POSTGRES_HOST": dir, "POSTGRES_PORT": "55440",
				"POSTGRES_USER": "restore_test", "POSTGRES_PASSWORD": "unused", "POSTGRES_DATABASE": "restored",
				"BACKUP_FILENAME_MODE": "fixed", "RESTORE_NO_TABLESPACES": tc.noTablespaces,
				"RESTORE_NO_TABLE_ACCESS_METHOD": tc.noAccessMethod,
			}
			config, err := loadConfig(func(key string) (string, bool) { v, ok := env[key]; return v, ok })
			if err != nil {
				t.Fatal(err)
			}
			var stderr bytes.Buffer
			app := New(config, &archiveDownloadStore{archive: archive}, io.Discard, &stderr)
			app.lockPath = filepath.Join(dir, "backup.lock")
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			err = app.Restore(ctx, nil)
			if tc.wantMethod == "" {
				if err == nil || !strings.Contains(err.Error(), "pg_restore failed") || strings.Contains(stderr.String(), "restore_space") {
					t.Fatalf("missing tablespace must fail without fallback: %v\n%s", err, &stderr)
				}
				if got := sql("restored", "SELECT v FROM probe"); got != "99" {
					t.Fatalf("failed restore changed existing data: %s", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("restore: %v\n%s", err, &stderr)
			}
			if got := sql("restored", "SELECT encode(float8send(v), 'hex') FROM probe"); got != wantBits {
				t.Fatalf("floating-point value changed: %s, want %s", got, wantBits)
			}
			got := sql("restored", "SELECT reltablespace, amname FROM pg_class JOIN pg_am ON relam=pg_am.oid WHERE pg_class.oid='probe'::regclass")
			if got != "0|"+tc.wantMethod {
				t.Fatalf("unexpected placement/access method: %s", got)
			}
		})
	}
}

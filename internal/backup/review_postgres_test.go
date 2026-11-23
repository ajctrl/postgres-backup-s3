package backup

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
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
	t.Run("snapshot-pins-client-endpoint", func(t *testing.T) {
		// This TCP forwarding port differs from the PostgreSQL backend's
		// Unix socket. inet_server_addr()/port cannot identify this endpoint.
		proxy, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer proxy.Close()
		go func() {
			for {
				client, err := proxy.Accept()
				if err != nil {
					return
				}
				go func() {
					defer client.Close()
					server, err := net.Dial("unix", filepath.Join(dir, ".s.PGSQL.55440"))
					if err != nil {
						return
					}
					defer server.Close()
					go func() {
						io.Copy(server, client)
						server.(*net.UnixConn).CloseWrite()
					}()
					io.Copy(client, server)
				}()
			}
		}()
		port := fmt.Sprint(proxy.Addr().(*net.TCPAddr).Port)
		conf := Config{Database: "postgres", Host: "unused.invalid,localhost", Port: "1," + port, User: "restore_test"}
		t.Setenv("PGHOSTADDR", "127.0.0.1,127.0.0.1")
		t.Setenv("PGSSLMODE", "disable")
		t.Setenv("PGCONNECT_TIMEOUT", "2")
		a := New(conf, nil, io.Discard, io.Discard)
		conn := a.dumpConnectionArgs("postgres", false)
		s, err := openCatalogSnapshot(t.Context(), a.runner(nil), conn, &dumpOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer s.close()
		e := s.endpoint
		if e.Host != "localhost" || e.Port != port || e.address != "127.0.0.1" {
			t.Fatalf("incorrect client endpoint: %+v", e)
		}
		// Conflicting fallback parameters force the wrong endpoint if the
		// database URI fails to pin the selected host, address and port.
		uri := e.connectionURI(conn[1])
		out, err := exec.CommandContext(t.Context(), filepath.Join(bin, "pg_dump"), "-h", "unused.invalid", "-p", "1", "-U", conf.User, "-d", uri, "--schema-only", "--snapshot="+s.id).CombinedOutput()
		if err != nil {
			t.Fatalf("pinned pg_dump failed: %v %s", err, out)
		}
	})

	t.Run("extension-without-plpgsql", func(t *testing.T) {
		sql("postgres", "CREATE DATABASE no_plpgsql")
		defer sql("postgres", "DROP DATABASE no_plpgsql")
		sql("no_plpgsql", "DROP EXTENSION plpgsql; CREATE EXTENSION hstore; CREATE TABLE marker(v int); INSERT INTO marker VALUES (17)")
		// Keep the stream open past the exporter's configured idle timeout.
		wrapper := t.TempDir()
		t.Setenv("REAL_DUMP", filepath.Join(bin, "pg_dump"))
		t.Setenv("PROBE_SOCKET", dir)
		script := `#!/bin/sh
set -eu
"$REAL_DUMP" "$@"
sleep 1
idle=$(psql -XAt -h "$PROBE_SOCKET" -p 55440 -U restore_test -d no_plpgsql -c "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND state='idle in transaction'")
test "$idle" = 0
`
		if err := os.WriteFile(filepath.Join(wrapper, "pg_dump"), []byte(script), 0700); err != nil {
			t.Fatal(err)
		}
		t.Setenv("PATH", wrapper+":"+os.Getenv("PATH"))
		t.Setenv("PGOPTIONS", "-c idle_in_transaction_session_timeout=250ms")
		c := Config{Database: "no_plpgsql", Host: dir, Port: "55440", User: "restore_test", BackupAll: "false", FilenameMode: "fixed"}
		store := &capturedBackupStore{}
		var log bytes.Buffer
		a := New(c, store, &log, &log)
		a.lockPath = filepath.Join(dir, "backup.lock")
		if err := a.Backup(t.Context()); err != nil {
			t.Fatalf("backup: %v\n%s", err, &log)
		}
		sql("no_plpgsql", "UPDATE marker SET v=29")
		a.Store = &archiveDownloadStore{archive: store.archive.Bytes()}
		if err := a.Restore(t.Context(), nil); err != nil {
			t.Fatalf("restore: %v\n%s", err, &log)
		}
		if got := sql("no_plpgsql", "SELECT v FROM marker"); got != "17" {
			t.Fatalf("restored marker = %s", got)
		}
		// Run the configuration-table and sequence guards without a procedural language.
		sql("no_plpgsql", `CREATE TABLE "cfg""table"(v int); CREATE SEQUENCE "cfg""seq";
ALTER EXTENSION hstore ADD TABLE "cfg""table"; ALTER EXTENSION hstore ADD SEQUENCE "cfg""seq";
UPDATE pg_extension SET extconfig=ARRAY['"cfg""table"'::regclass::oid,'"cfg""seq"'::regclass::oid], extcondition=ARRAY['',''] WHERE extname='hstore'`)
		for _, item := range []extensionTable{{namespace: "public", name: `cfg"table`, owner: "hstore", kind: "TABLE"}, {namespace: "public", name: `cfg"seq`, owner: "hstore", kind: "SEQUENCE"}} {
			sql("no_plpgsql", "BEGIN; "+beginExtensionRows(item)+" ROLLBACK;")
		}
	})

	t.Run("truncated-database-name", func(t *testing.T) {
		name := strings.Repeat("x", 63)
		sql("postgres", "CREATE DATABASE "+name)
		defer sql("postgres", "DROP DATABASE "+name)
		sql(name, "CREATE TABLE marker(v int); INSERT INTO marker VALUES (17)")
		archive := run("pg_dump", append(append([]string{}, connection...), "-d", name, "-Fc")...)
		sql(name, "UPDATE marker SET v=29")
		c := Config{Database: name + "extra", Host: dir, Port: "55440", User: "restore_test", BackupAll: "false", FilenameMode: "fixed"}
		var log bytes.Buffer
		store := &capturedBackupStore{}
		app := New(c, store, &log, &log)
		app.lockPath = filepath.Join(dir, "backup.lock")
		if err := app.Backup(t.Context()); err == nil || store.published != 0 || !strings.Contains(log.String(), "does not match requested name") {
			t.Fatalf("misnamed backup: %v %s", err, &log)
		}
		restore := New(c, &archiveDownloadStore{archive: archive}, &log, &log)
		restore.lockPath = app.lockPath
		if err := restore.restoreFrom(t.Context(), nil, name); err == nil || !strings.Contains(err.Error(), "does not match requested name") || sql(name, "SELECT v FROM marker") != "29" {
			t.Fatalf("misdirected restore: %v %s", err, &log)
		}
	})

	t.Run("service-file-cannot-disable-required-tls", func(t *testing.T) {
		service := filepath.Join(t.TempDir(), "pg_service.conf")
		if err := os.WriteFile(service, []byte("[review]\nsslmode=disable\n"), 0600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("PGSERVICEFILE", service)
		t.Setenv("PGSERVICE", "review")
		t.Setenv("PGGSSENCMODE", "disable")
		t.Setenv("PGSSLMODE", "verify-full")
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		request := make(chan uint32, 1)
		go func() {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(5 * time.Second))
			var header [8]byte
			if _, err := io.ReadFull(conn, header[:]); err != nil {
				return
			}
			request <- binary.BigEndian.Uint32(header[4:])
			// PostgreSQL replies N to an SSLRequest when TLS is disabled.
			conn.Write([]byte("N"))
		}()
		a := New(Config{Host: "127.0.0.1", Port: fmt.Sprint(listener.Addr().(*net.TCPAddr).Port), User: "restore_test"}, nil, io.Discard, io.Discard)
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, filepath.Join(bin, "psql"), append(a.connectionArgs("postgres"), "-c", "SELECT 1")...)
		if err := command.Run(); err == nil {
			t.Fatal("accepted a server without TLS")
		}
		select {
		case code := <-request:
			if code != 80877103 {
				t.Fatalf("expected SSLRequest, got %d", code)
			}
		case <-ctx.Done():
			t.Fatal("missing connection attempt")
		}
	})

	t.Run("foreign-data-is-never-replayed", func(t *testing.T) {
		sql("postgres", "CREATE DATABASE fdw_upstream")
		defer sql("postgres", "DROP DATABASE fdw_upstream")
		sql("postgres", "CREATE DATABASE fdw_source")
		defer sql("postgres", "DROP DATABASE fdw_source")
		sql("postgres", "CREATE DATABASE fdw_recovery")
		defer sql("postgres", "DROP DATABASE fdw_recovery")
		sql("fdw_upstream", "CREATE TABLE records(v int); INSERT INTO records VALUES (1)")
		sql("fdw_source", "CREATE EXTENSION postgres_fdw; CREATE SERVER rem FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '"+strings.ReplaceAll(dir, "'", "''")+"', port '55440', dbname 'fdw_upstream'); CREATE USER MAPPING FOR restore_test SERVER rem OPTIONS (user 'restore_test'); CREATE FOREIGN TABLE external_records(v int) SERVER rem OPTIONS (table_name 'records')")
		filter := filepath.Join(t.TempDir(), "filter")
		if err := os.WriteFile(filter, []byte("include foreign_data rem\n"), 0600); err != nil {
			t.Fatal(err)
		}
		c := Config{Database: "fdw_source", Host: dir, Port: "55440", User: "restore_test", BackupAll: "false", FilenameMode: "fixed", AllowPartial: true, DumpOptions: "--filter=" + filter}
		s := &capturedBackupStore{}
		var log bytes.Buffer
		app := New(c, s, &log, &log)
		app.lockPath = filepath.Join(dir, "backup.lock")
		if err := app.Backup(t.Context()); err == nil || s.published != 0 || !strings.Contains(log.String(), "foreign table data") {
			t.Fatalf("unsafe dump: %v %s", err, &log)
		}
		dump := run("pg_dump", append(append([]string{}, connection...), "-d", "fdw_source", "--format=custom", "--filter="+filter)...)
		c.Database = "fdw_recovery"
		app = New(c, &archiveDownloadStore{archive: dump}, io.Discard, io.Discard)
		app.lockPath = filepath.Join(dir, "backup.lock")
		err := app.Restore(t.Context(), nil)
		if err == nil || !strings.Contains(err.Error(), "foreign table data") || sql("fdw_upstream", "SELECT count(*) FROM records") != "1" || sql("fdw_recovery", "SELECT count(*) FROM pg_foreign_server") != "0" {
			t.Fatalf("unsafe restore: %v", err)
		}
		sql("fdw_source", `ALTER EXTENSION postgres_fdw ADD FOREIGN TABLE external_records;
UPDATE pg_extension SET extconfig=ARRAY['external_records'::regclass::oid], extcondition=ARRAY[''] WHERE extname='postgres_fdw'`)
		log.Reset()
		c.Database = "fdw_source"
		app = New(c, s, &log, &log)
		app.lockPath = filepath.Join(dir, "backup.lock")
		if err := app.Backup(t.Context()); err == nil || s.published != 0 || !strings.Contains(log.String(), "verified local configuration relation") || !strings.Contains(log.String(), "external_records") {
			t.Fatalf("extension-owned foreign data: %v %s", err, &log)
		}
	})

	t.Run("extension-configuration-round-trip", func(t *testing.T) {
		sql("postgres", "CREATE DATABASE config_source")
		defer sql("postgres", "DROP DATABASE config_source")
		sql("config_source", `CREATE EXTENSION hstore VERSION '1.4';
CREATE TABLE settings(v int); INSERT INTO settings VALUES (42);
ALTER EXTENSION hstore ADD TABLE settings;
CREATE SEQUENCE config_seq; ALTER EXTENSION hstore ADD SEQUENCE config_seq;
SELECT setval('config_seq', 42, true);
UPDATE pg_extension SET extconfig=ARRAY['settings'::regclass::oid,'config_seq'::regclass::oid], extcondition=ARRAY['',''] WHERE extname='hstore';
CREATE TABLE upstream(v int); SELECT lo_create(49001);`)
		store := &capturedBackupStore{}
		var log bytes.Buffer
		c := Config{Database: "config_source", BackupAll: "false", Host: dir, Port: "55440", User: "restore_test", FilenameMode: "fixed"}
		app := New(c, store, &log, &log)
		app.lockPath = filepath.Join(dir, "backup.lock")
		if err := app.Backup(t.Context()); err != nil || store.published != 1 {
			t.Fatalf("ordinary configuration data: %v %s", err, &log)
		}
		t.Run("two-connections", func(t *testing.T) {
			sql("postgres", "CREATE ROLE config_reader LOGIN CONNECTION LIMIT 2; GRANT pg_read_all_data TO config_reader")
			defer sql("postgres", "DROP ROLE config_reader")
			sql("config_source", "GRANT SELECT ON LARGE OBJECT 49001 TO config_reader; CREATE TABLE bulk AS SELECT md5(i::text) AS v FROM generate_series(1,40000) i")
			defer sql("config_source", "REVOKE SELECT ON LARGE OBJECT 49001 FROM config_reader; DROP TABLE bulk")
			limited := c
			limited.User = "config_reader"
			single := &capturedBackupStore{}
			backup := New(limited, single, &log, &log)
			backup.lockPath = app.lockPath
			if err := backup.Backup(t.Context()); err != nil || single.published != 1 {
				t.Fatalf("single connection: %v %s", err, &log)
			}
		})
		var saved archiveMetadata
		if err := inspectArchive(t.Context(), bytes.NewReader(store.archive.Bytes()), archiveChecks{metadata: &saved}); err != nil || len(saved.extensionTables) != 2 {
			t.Fatalf("missing extension data entry: %v", err)
		}
		// An OID that no longer matches the archive must fail closed, even if
		// a configuration table with the archived name still exists.
		path := filepath.Join(t.TempDir(), "verify.dump")
		if err := os.WriteFile(path, store.archive.Bytes(), 0600); err != nil {
			t.Fatal(err)
		}
		proofFile, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer proofFile.Close()
		if err := readCatalogProof(proofFile, &saved); err != nil {
			t.Fatal(err)
		}
		for i := range saved.extensionTables {
			if saved.extensionTables[i].kind == "TABLE" {
				saved.extensionTables[i].oid = "1"
			}
		}
		if err := matchCatalog(&saved); err == nil || !strings.Contains(err.Error(), "settings") {
			t.Fatalf("unverified source table: %v", err)
		}
		// Model two installed versions of the extension. The table is created
		// by CREATE EXTENSION, after the target catalog preflight.
		for _, foreign := range []bool{false, true} {
			t.Run(fmt.Sprintf("foreign=%t", foreign), func(t *testing.T) {
				sql("postgres", "CREATE DATABASE config_recovery")
				defer sql("postgres", "DROP DATABASE config_recovery")
				create := "CREATE TABLE public.settings(v int); ALTER EXTENSION hstore ADD TABLE public.settings;"
				if foreign {
					sql("config_recovery", "CREATE EXTENSION postgres_fdw; CREATE SERVER rem FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '"+strings.ReplaceAll(dir, "'", "''")+"', port '55440', dbname 'config_source'); CREATE USER MAPPING FOR restore_test SERVER rem OPTIONS (user 'restore_test')")
					create = "CREATE FOREIGN TABLE public.settings(v int) SERVER rem OPTIONS (table_name 'upstream'); ALTER EXTENSION hstore ADD FOREIGN TABLE public.settings;"
				}
				sql("config_recovery", `CREATE FUNCTION public.install_settings() RETURNS event_trigger LANGUAGE plpgsql AS $$BEGIN
IF EXISTS (SELECT FROM pg_catalog.pg_extension WHERE extname='hstore') AND pg_catalog.to_regclass('public.settings') IS NULL THEN `+create+`
CREATE SEQUENCE public.config_seq; ALTER EXTENSION hstore ADD SEQUENCE public.config_seq;
UPDATE pg_catalog.pg_extension SET extconfig=ARRAY['public.settings'::regclass::oid,'public.config_seq'::regclass::oid], extcondition=ARRAY['',''] WHERE extname='hstore';
END IF; END$$;
CREATE EVENT TRIGGER config_install ON ddl_command_end WHEN TAG IN ('CREATE EXTENSION') EXECUTE FUNCTION public.install_settings();`)
				if foreign {
					sql("config_recovery", "CREATE EXTENSION hstore")
					if got := sql("config_recovery", "SELECT count(*) FROM public.settings"); got != "0" {
						t.Fatalf("foreign fixture could not access upstream: %q", got)
					}
					sql("config_recovery", "DROP EXTENSION hstore")
				}
				target := c
				target.Database = "config_recovery"
				restore := New(target, &archiveDownloadStore{archive: store.archive.Bytes()}, &log, &log)
				restore.lockPath = app.lockPath
				err := restore.restoreFrom(t.Context(), nil, "config_source")
				if foreign {
					if err == nil || !strings.Contains(err.Error(), "pg_restore failed") || sql("config_source", "SELECT count(*) FROM upstream") != "0" || sql("config_recovery", "SELECT count(*) FROM pg_extension WHERE extname='hstore'") != "0" {
						t.Fatalf("foreign mapping was not rolled back: %v", err)
					}
				} else if err != nil || sql("config_recovery", "SELECT extversion FROM pg_extension WHERE extname='hstore'") != "1.4" || sql("config_recovery", "SELECT nextval('config_seq')") != "43" || sql("config_recovery", "SELECT v FROM settings") != "42" || sql("config_recovery", "SELECT count(*) FROM pg_largeobject_metadata WHERE oid=49001") != "1" {
					t.Fatalf("configuration rows and large object: %v %s", err, &log)
				}
				if !foreign {
					// A seekable archive records absolute data offsets. Exercise both
					// adjusted offsets and INSERT rows before a later FDW refresh.
					sql("config_source", "CREATE EXTENSION postgres_fdw; CREATE SERVER rem FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host '"+strings.ReplaceAll(dir, "'", "''")+"',port '55440',dbname 'config_source'); CREATE USER MAPPING FOR restore_test SERVER rem OPTIONS (user 'restore_test'); CREATE FOREIGN TABLE remote_settings(v int) SERVER rem OPTIONS (table_name 'settings'); CREATE MATERIALIZED VIEW snapshot_settings AS SELECT v FROM remote_settings")
					source := c
					source.DumpOptions = "--inserts"
					withView := &capturedBackupStore{}
					b := New(source, withView, &log, &log)
					b.lockPath = app.lockPath
					if err := b.Backup(t.Context()); err != nil {
						t.Fatal(err, log.String())
					}
					// The app streams a non-seekable dump. Also exercise absolute offsets
					// from a native seekable dump of this unchanged private database.
					file := filepath.Join(t.TempDir(), "seekable.dump")
					run("pg_dump", append(append([]string{}, connection...), "-d", "config_source", "-Fc", "--inserts", "-f", file)...)
					native, err := os.ReadFile(file)
					if err != nil {
						t.Fatal(err)
					}
					metadataFile := filepath.Join(t.TempDir(), "streamed.dump")
					if err := os.WriteFile(metadataFile, withView.archive.Bytes(), 0600); err != nil {
						t.Fatal(err)
					}
					input, err := os.OpenFile(metadataFile, os.O_RDWR, 0)
					if err != nil {
						t.Fatal(err)
					}
					defer input.Close()
					var nativeMetadata archiveMetadata
					if err := validateRestoreArchive(t.Context(), input, &nativeMetadata); err != nil {
						t.Fatal(err)
					}
					if err := readCatalogProof(input, &nativeMetadata); err != nil {
						t.Fatal(err)
					}
					var packed bytes.Buffer
					packed.Write(native)
					if err := writeCatalogProof(&packed, nativeMetadata.proofs); err != nil {
						t.Fatal(err)
					}
					restore.Store = &archiveDownloadStore{archive: packed.Bytes()}
					if err := restore.restoreFrom(t.Context(), nil, "config_source"); err != nil || sql("config_recovery", "SELECT v FROM snapshot_settings") != "42" || sql("config_recovery", "SELECT v FROM settings") != "42" {
						t.Fatalf("read-only FDW refresh after configuration data: %v %s", err, &log)
					}
				}
			})
		}
	})
	t.Run("materialized-view-refresh", func(t *testing.T) {
		sql("postgres", "CREATE DATABASE cached_view")
		defer sql("postgres", "DROP DATABASE cached_view")
		sql("cached_view", "CREATE TABLE src(v int); INSERT INTO src VALUES (1); CREATE MATERIALIZED VIEW snap AS SELECT v FROM src; UPDATE src SET v=2")
		if sql("cached_view", "SELECT v FROM snap") != "1" {
			t.Fatal("view should be stale before backup")
		}
		dump := run("pg_dump", append(append([]string{}, connection...), "-d", "cached_view", "--format=custom")...)
		var log bytes.Buffer
		app := New(Config{Database: "cached_view", Host: dir, Port: "55440", User: "restore_test", FilenameMode: "fixed"}, &archiveDownloadStore{archive: dump}, &log, &log)
		app.lockPath = filepath.Join(dir, "backup.lock")
		if err := app.Restore(t.Context(), nil); err != nil || !strings.Contains(log.String(), "recompute materialized views") || sql("cached_view", "SELECT v FROM snap") != "2" {
			t.Fatalf("restore: %v %s", err, &log)
		}
	})

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

	t.Run("target-subscription-with-slot", func(t *testing.T) {
		sql("postgres", "CREATE DATABASE subscription_target")
		defer sql("postgres", "DROP DATABASE subscription_target")
		sql("subscription_target", "CREATE TABLE probe(v int); INSERT INTO probe VALUES (1)")
		sql("subscription_target", "CREATE SUBSCRIPTION abonn\u00e9 CONNECTION 'dbname=postgres' PUBLICATION pub WITH (connect=false, slot_name='slot_probe')")
		defer sql("subscription_target", "DROP SUBSCRIPTION abonn\u00e9")
		defer sql("subscription_target", "ALTER SUBSCRIPTION abonn\u00e9 SET (slot_name=NONE)")
		archive := run("pg_dump", append(append([]string{}, connection...), "-d", "subscription_target", "--format=custom", "--encoding=LATIN1")...)
		sql("subscription_target", "UPDATE probe SET v = 2")
		app := New(Config{Database: "subscription_target", Host: dir, Port: "55440", User: "restore_test", FilenameMode: "fixed"},
			&archiveDownloadStore{archive: archive}, io.Discard, io.Discard)
		app.lockPath = filepath.Join(dir, "backup.lock")
		err := app.Restore(t.Context(), nil)
		if err == nil || !strings.Contains(err.Error(), "associated replication slot") || sql("subscription_target", "SELECT v FROM probe") != "2" {
			t.Fatalf("expected preflight rejection with target rows intact: %v", err)
		}
	})

	t.Run("lock-timeout-continues-with-next-database", func(t *testing.T) {
		sql("postgres", "CREATE DATABASE locked_source")
		defer sql("postgres", "DROP DATABASE locked_source")
		sql("locked_source", "CREATE TABLE probe(id integer)")
		archive := run("pg_dump", append(append([]string{}, connection...), "-Fc", "-d", "locked_source")...)
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
		restore := New(Config{Database: "locked_source", Host: dir, Port: "55440", User: "restore_test", FilenameMode: "fixed"},
			&archiveDownloadStore{archive: archive}, io.Discard, io.Discard)
		restore.lockPath = filepath.Join(dir, "backup.lock")
		t.Setenv("PGAPPNAME", "pgs3_cancel_test")
		restoreCtx, stopRestore := context.WithCancel(ctx)
		defer stopRestore()
		restoreDone := make(chan error, 1)
		go func() { restoreDone <- restore.Restore(restoreCtx, nil) }()
		waiting := func() bool {
			return sql("postgres", "SELECT EXISTS(SELECT FROM pg_stat_activity WHERE application_name='pgs3_cancel_test' AND wait_event_type='Lock')") == "t"
		}
		limit := time.Now().Add(5 * time.Second)
		for !waiting() {
			if time.Now().After(limit) {
				t.Fatal("restore did not reach server lock wait")
			}
			time.Sleep(20 * time.Millisecond)
		}
		stopRestore()
		if err := <-restoreDone; err == nil {
			t.Fatal("canceled restore succeeded")
		}
		if waiting() {
			t.Fatal("canceled restore left a pending PostgreSQL lock request")
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
		t.Run("service-file", func(t *testing.T) {
			service := filepath.Join(t.TempDir(), "pg_service.conf")
			if err := os.WriteFile(service, []byte("[discovery]\nclient_encoding=LATIN1\n"), 0600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PGSERVICEFILE", service)
			t.Setenv("PGSERVICE", "discovery")
			names, err := a.databases(t.Context(), commandRunner{stderr: io.Discard})
			if err != nil || !slices.Contains(names, "caf\u00e9") {
				t.Fatalf("service encoding changed discovery: %q %v", names, err)
			}
		})
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
		sql("postgres", "CREATE ROLE mapping_reader")
		defer sql("postgres", "DROP ROLE mapping_reader")
		sql("postgres", "GRANT pg_read_all_data TO mapping_reader")
		for _, tc := range []struct {
			role             string
			partial, success bool
		}{
			{"restore_test", false, true},
			{"mapping_reader", false, false},
			{"mapping_reader", true, true},
		} {
			var log bytes.Buffer
			s := &capturedBackupStore{}
			app := New(Config{Database: "mapping_source", BackupAll: "false", Host: dir, Port: "55440", User: "restore_test",
				DumpOptions: "--role=" + tc.role, AllowPartial: tc.partial, FilenameMode: "timestamp"}, s, &log, &log)
			app.lockPath = filepath.Join(dir, "backup.lock")
			err := app.Backup(t.Context())
			if err == nil && tc.role == "restore_test" && !tc.partial {
				archive = append([]byte(nil), s.archive.Bytes()...)
			}
			if (err == nil) != tc.success || (s.published == 1) != tc.success ||
				(!tc.success && !strings.Contains(log.String(), "USER MAPPING has no visible options")) ||
				strings.Contains(log.String(), "synthetic-review-secret") {
				t.Fatalf("role=%s partial=%v published=%d err=%v", tc.role, tc.partial, s.published, err)
			}
		}
		sql("mapping_source", "ALTER USER MAPPING FOR mapping_owner SERVER mapping_server OPTIONS (DROP password)")
		empty := &capturedBackupStore{}
		var emptyLog bytes.Buffer
		backup := New(Config{Database: "mapping_source", BackupAll: "false", Host: dir, Port: "55440", User: "restore_test", FilenameMode: "timestamp"}, empty, &emptyLog, &emptyLog)
		backup.lockPath = filepath.Join(dir, "backup.lock")
		if err := backup.Backup(t.Context()); err != nil || empty.published != 1 {
			t.Fatalf("genuinely empty mapping: %v %s", err, &emptyLog)
		}
		sql("postgres", "ALTER ROLE mapping_owner LOGIN")
		sql("mapping_source", "GRANT SELECT ON probe TO mapping_owner; GRANT USAGE ON FOREIGN SERVER mapping_server TO mapping_owner")
		t.Run("login-can-prove-hidden-mapping-is-empty", func(t *testing.T) {
			conf := backup.Config
			conf.DumpOptions = "--role=mapping_reader"
			a := New(conf, &capturedBackupStore{}, &emptyLog, &emptyLog)
			a.lockPath = backup.lockPath
			if err := a.Backup(t.Context()); err != nil {
				t.Fatalf("catalog proof from login: %v %s", err, &emptyLog)
			}
		})
		t.Run("mapping-uses-effective-role", func(t *testing.T) {
			sql("postgres", "CREATE ROLE mapping_login LOGIN NOINHERIT; GRANT mapping_owner TO mapping_login")
			defer sql("postgres", "DROP ROLE mapping_login")
			conf := backup.Config
			conf.User = "mapping_login"
			conf.DumpOptions = "--role=mapping_owner"
			s := &capturedBackupStore{}
			a := New(conf, s, &emptyLog, &emptyLog)
			a.lockPath = backup.lockPath
			if err := a.Backup(t.Context()); err != nil || s.published != 1 {
				t.Fatalf("mapped role backup: %v %s", err, &emptyLog)
			}
		})
		t.Run("mapping-uses-requested-snapshot", func(t *testing.T) {
			outer, err := openCatalogSnapshot(t.Context(), backup.runner(nil), backup.dumpConnectionArgs("mapping_source", false), &dumpOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer outer.close()
			sql("mapping_source", "ALTER USER MAPPING FOR mapping_owner SERVER mapping_server OPTIONS (ADD user 'later')")
			defer sql("mapping_source", "ALTER USER MAPPING FOR mapping_owner SERVER mapping_server OPTIONS (DROP user)")
			config := backup.Config
			config.DumpOptions = "--snapshot=" + outer.id
			store := &capturedBackupStore{}
			a := New(config, store, &emptyLog, &emptyLog)
			a.lockPath = filepath.Join(dir, "backup.lock")
			if err := a.Backup(t.Context()); err != nil || store.published != 1 {
				t.Fatalf("old empty mapping snapshot: %v %s", err, &emptyLog)
			}
		})
		backup.Config.User = "mapping_owner"
		if err := backup.Backup(t.Context()); err != nil || empty.published != 2 {
			t.Fatalf("mapped user with catalog-view visibility: %v %s", err, &emptyLog)
		}
		sql("mapping_source", "REVOKE SELECT ON probe FROM mapping_owner; REVOKE USAGE ON FOREIGN SERVER mapping_server FROM mapping_owner")
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
		err = a.restoreFrom(ctx, nil, "mapping_source")
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
		if err := inspectArchive(t.Context(), bytes.NewReader(sequenceArchive), archiveChecks{rejectUnlogged: true}); err == nil || !strings.Contains(err.Error(), "UNLOGGED") {
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
				if err := restore.restoreFrom(ctx, nil, tc.db); err != nil {
					t.Fatalf("restore: %v\n%s", err, &log)
				}
				if got := sql("standby_restored", "SELECT count(*) FROM logged_probe"); got != "1" {
					t.Fatalf("logged data lost: %s", got)
				}
				if tc.unloggedRows != "" {
					if got := sql("standby_restored", "SELECT count(*) FROM unlogged_probe"); got != tc.unloggedRows {
						t.Fatalf("unexpected UNLOGGED contents: %s, want %s", got, tc.unloggedRows)
					}
					if err := inspectArchive(ctx, bytes.NewReader(store.archive.Bytes()), archiveChecks{rejectUnlogged: true}); err == nil || !strings.Contains(err.Error(), "UNLOGGED") {
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
			originalPath := os.Getenv("PATH")
			t.Setenv("PATH", wrapper+string(os.PathListSeparator)+originalPath)
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
			t.Setenv("PATH", originalPath) // Only source preflight uses the simulated failover.
			sql("postgres", "CREATE DATABASE role_restored")
			defer sql("postgres", "DROP DATABASE role_restored")
			restore := New(Config{Database: "role_restored", Host: dir, Port: "55440", User: "restore_test", FilenameMode: "fixed"},
				&archiveDownloadStore{archive: store.archive.Bytes()}, &log, &log)
			restore.lockPath = a.lockPath
			if err := restore.restoreFrom(ctx, nil, "unlogged_source"); err != nil {
				t.Fatalf("restore: %v\n%s", err, &log)
			}
			if got := sql("role_restored", "SELECT count(*) FROM unlogged_probe"); got != tc.wantRows {
				t.Fatalf("dump used the wrong role: UNLOGGED rows=%s want=%s", got, tc.wantRows)
			}
		})
	}
}

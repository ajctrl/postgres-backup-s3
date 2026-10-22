package backup

import (
	"context"
	"io"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// Reuse the private cluster so this regression also runs in the required
// PostgreSQL 17/18 CI tests. The attacker has schema CREATE, not table access.
func testMetadataQueriesWithPostgreSQL(t *testing.T, dir, bin string) {
	t.Helper()
	t.Run("metadata-search-path", func(t *testing.T) {
		sql := func(database, query string) string {
			t.Helper()
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			out, err := exec.CommandContext(ctx, filepath.Join(bin, "psql"),
				"-X", "-At", "-v", "ON_ERROR_STOP=1", "-h", dir, "-p", "55440",
				"-U", "restore_test", "-d", database, "-c", query).CombinedOutput()
			if err != nil {
				t.Fatalf("metadata fixture SQL failed: %v\n%s", err, out)
			}
			return strings.TrimSpace(string(out))
		}
		sql("postgres", "CREATE DATABASE metadata_source")
		defer sql("postgres", "DROP DATABASE metadata_source")
		sql("postgres", "CREATE DATABASE metadata_hidden")
		defer sql("postgres", "DROP DATABASE metadata_hidden")
		sql("postgres", "CREATE ROLE metadata_attacker")
		defer func() {
			sql("metadata_source", "DROP OWNED BY metadata_attacker")
			sql("postgres", "DROP ROLE metadata_attacker")
		}()
		sql("metadata_source", `CREATE TABLE public.metadata_marker(actor text);
GRANT CREATE ON SCHEMA public TO metadata_attacker;
SET ROLE metadata_attacker;
DO $body$ BEGIN
  BEGIN
    INSERT INTO public.metadata_marker VALUES (current_user);
    RAISE EXCEPTION 'attacker unexpectedly has direct table access';
  EXCEPTION WHEN insufficient_privilege THEN NULL;
  END;
END $body$;
CREATE FUNCTION public.json_build_object(text, boolean, text, boolean) RETURNS json
LANGUAGE plpgsql AS $body$ BEGIN
  INSERT INTO public.metadata_marker VALUES (current_user);
  RETURN '{"standby":false,"unlogged":false}'::json;
END $body$;
CREATE FUNCTION public.metadata_aggregate(json, name) RETURNS json
LANGUAGE plpgsql AS $body$ BEGIN
  INSERT INTO public.metadata_marker VALUES (current_user);
  RETURN '["metadata_source"]'::json;
END $body$;
CREATE AGGREGATE public.json_agg(name)
  (SFUNC=public.metadata_aggregate, STYPE=json, INITCOND='[]');`)
		for _, mode := range []string{"default", "role-default", "environment"} {
			t.Run(mode, func(t *testing.T) {
				sql("metadata_source", "TRUNCATE public.metadata_marker")
				sql("postgres", "ALTER ROLE restore_test IN DATABASE metadata_source RESET search_path")
				options := "-c statement_timeout=5000"
				if mode == "role-default" {
					sql("postgres", "ALTER ROLE restore_test IN DATABASE metadata_source SET search_path = public, pg_catalog")
				}
				if mode == "environment" {
					options += " -c search_path=public,pg_catalog"
				}
				t.Setenv("PGOPTIONS", options)
				t.Setenv("PGAPPNAME", "metadata-probe")
				a := New(Config{Host: dir, Port: "55440", User: "restore_test", BackupAll: "true",
					MaintenanceDB: "metadata_source"}, nil, io.Discard, io.Discard)
				runner := commandRunner{stderr: io.Discard}
				standby, err := a.checkBackupSource(t.Context(), runner, "metadata_source")
				if err != nil || standby {
					t.Errorf("source check: standby=%v err=%v", standby, err)
				}
				names, err := a.databases(t.Context(), runner)
				if err != nil || !slices.Contains(names, "metadata_source") || !slices.Contains(names, "metadata_hidden") {
					t.Errorf("database discovery was altered: %v, %v", names, err)
				}
				if actors := sql("metadata_source", "SELECT actor FROM public.metadata_marker"); actors != "" {
					t.Errorf("attacker functions ran as: %s", actors)
				}
				// Protect the whole metadata session, while preserving other libpq
				// options and producing exactly one JSON result (no SET command tag).
				var settings struct{ Path, App, Timeout string }
				args := append([]string{"-X", "-At", "-v", "ON_ERROR_STOP=1"}, a.connectionArgs("metadata_source")...)
				args = append(args, "-c", `SELECT pg_catalog.json_build_object(
'Path', pg_catalog.current_setting('search_path'),
'App', pg_catalog.current_setting('application_name'),
'Timeout', pg_catalog.current_setting('statement_timeout'))`)
				if err := runner.queryJSON(t.Context(), &settings, args...); err != nil {
					t.Fatal(err)
				}
				if settings.Path != "pg_catalog, pg_temp" || settings.App != "metadata-probe" || settings.Timeout != "5s" {
					t.Errorf("unsafe search path or lost connection options: %+v", settings)
				}
			})
		}
	})
}

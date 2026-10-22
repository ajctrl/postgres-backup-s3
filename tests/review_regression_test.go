package tests

import (
	"strings"
	"testing"
)

func TestMissingServerDateKeepsBackupAndReportsRetentionFailure(t *testing.T) {
	for _, date := range []string{"", "invalid"} {
		t.Run(date, func(t *testing.T) {
			f := newFixture(t)
			f.env["POSTGRES_DATABASE"], f.env["BACKUP_KEEP_DAYS"] = "app", "7"
			f.s3.listDate = &date
			f.s3.objects = []object{{Key: "backup/app/2000-01-01T00:00:00-0123456789abcdef0123456789abcdef.dump", LastModified: "2000-01-01T00:00:00Z"}}
			out := f.run(false, "backup")
			if !strings.Contains(out, "1 succeeded, 0 failed, 1 retention cleanups failed") || !strings.Contains(out, "no valid server Date") {
				t.Fatalf("incorrect retention status: %s", out)
			}
			assertEqual(t, len(f.s3.calls("PUT")), 1)
			assertEqual(t, len(f.s3.calls("DELETE")), 0)
			// Restore selection needs no retention clock.
			f.run(true, "restore")
			assertEqual(t, len(f.s3.calls("GET")), 1)
		})
	}
}

func TestSourceCheckFailureStopsOnlyAffectedDatabase(t *testing.T) {
	f := newFixture(t)
	f.env["POSTGRES_DATABASES"], f.env["BACKUP_KEEP_DAYS"] = "app,billing", "7"
	f.env["FAKE_SOURCE_FAILURE"] = "postgresql:///app"
	out := f.run(false, "backup")
	if !strings.Contains(out, "1 succeeded, 1 failed") || !strings.Contains(out, "cannot verify backup source") {
		t.Fatalf("incorrect source failure status: %s", out)
	}
	assertEqual(t, f.databaseCalls("psql"), []string{"app", "billing"})
	assertEqual(t, f.databaseCalls("pg_dump"), []string{"billing"})
	assertEqual(t, len(f.s3.calls("PUT")), 1)
	lists := f.s3.calls("list")
	assertEqual(t, len(lists), 1)
	assertEqual(t, lists[0].Query.Get("prefix"), "backup/billing/")
}

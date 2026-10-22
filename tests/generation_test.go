package tests

import (
	"strings"
	"testing"
)

func TestDistinctGenerationsAndExplicitRestore(t *testing.T) {
	f := newFixture(t)
	f.env["POSTGRES_DATABASE"] = "app"
	f.run(true, "backup")
	f.run(true, "backup")
	puts := f.s3.calls("PUT")
	if len(puts) != 2 || puts[0].Key == puts[1].Key {
		t.Fatalf("generations were not distinct: %v", keys(puts))
	}
	for _, put := range puts {
		assertEqual(t, put.IfNoneMatch, "*")
		generation := strings.TrimSuffix(strings.TrimPrefix(put.Key, "backup/app/"), ".dump")
		f.run(true, "restore", generation)
	}
	assertEqual(t, keys(f.s3.calls("GET")), keys(puts))
}

func TestGenerationCollisionSkipsRetention(t *testing.T) {
	f := newFixture(t)
	f.env["POSTGRES_DATABASE"], f.env["BACKUP_KEEP_DAYS"] = "app", "7"
	f.s3.fail, f.s3.failCode = "PUT", "PreconditionFailed"
	output := f.run(false, "backup")
	if !strings.Contains(output, "PreconditionFailed") || strings.Contains(output, "Backup complete") {
		t.Fatalf("collision incorrectly reported: %s", output)
	}
	assertEqual(t, f.s3.calls("PUT")[0].IfNoneMatch, "*")
	assertEqual(t, len(f.s3.calls("list")), 0)
	assertEqual(t, len(f.s3.calls("DELETE")), 0)
}

func TestLatestGenerationUsesModificationTimeAndIgnoresLegacyKeys(t *testing.T) {
	f := newFixture(t)
	f.env["POSTGRES_DATABASE"] = "app"
	want := "backup/app/2026-10-22T10:00:00-00000000000000000000000000000000.dump"
	f.s3.objects = []object{
		{Key: want, LastModified: "2026-10-22T10:00:02Z"},
		{Key: "backup/app/2026-10-22T10:00:00-ffffffffffffffffffffffffffffffff.dump", LastModified: "2026-10-22T10:00:01Z"},
		{Key: "backup/app/2026-10-22T11:00:00.dump", LastModified: "2026-10-22T11:00:00Z"},
	}
	f.run(true, "restore")
	assertEqual(t, keys(f.s3.calls("GET")), []string{want})
}

func TestLegacyTimestampRestoreRejectedBeforeServiceAccess(t *testing.T) {
	f := newFixture(t)
	f.env["POSTGRES_DATABASE"] = "app"
	f.run(false, "restore", "2026-10-22T10:00:00")
	assertEqual(t, len(f.s3.calls("")), 0)
	assertEqual(t, len(f.processes("")), 0)
}

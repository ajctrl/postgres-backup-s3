package tests

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestFallbackRestoreCommand(t *testing.T) {
	const database = "team's $archive"
	f := newFixture(t)
	f.env["POSTGRES_DATABASES"], f.env["BACKUP_FILENAME_MODE"] = database+",billing", "fixed"
	f.env["PASSPHRASE"] = "test"
	f.s3.versioning = "Suspended"
	f.s3.uploadFailure = "billing"
	log := f.run(false, "backup")
	assertEqual(t, strings.Contains(log, "1 succeeded, 1 failed"), true)
	const prefix = "Restore in backup container: "
	assertEqual(t, strings.Count(log, prefix), 1)
	_, rest, _ := strings.Cut(log, prefix)
	command, _, _ := strings.Cut(rest, "\n")
	puts := f.s3.calls("PUT")
	assertEqual(t, len(puts), 2)
	assertEqual(t, strings.HasSuffix(puts[0].Key, ".dump.gpg"), true)

	// Serve the bytes captured from the successful upload for the restore.
	restore := newFixture(t)
	restore.s3.downloadBody = []byte(puts[0].Payload)
	restore.env["BACKUP_FILENAME_MODE"], restore.env["PASSPHRASE"] = "fixed", f.env["PASSPHRASE"]
	restore.env["PATH"] = filepath.Dir(cliBinary) + ":" + restore.env["PATH"]
	cmd := exec.Command("sh", "-ec", command)
	cmd.Env, cmd.Dir = restore.command().Env, restore.dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("logged restore command failed: %v\n%s", err, out)
	}
	assertEqual(t, keys(restore.s3.calls("GET")), []string{puts[0].Key})
	assertEqual(t, len(restore.s3.calls("list")), 0)
	assertEqual(t, restore.databaseCalls("pg_restore"), []string{database})
	restore.assertClean()
}

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

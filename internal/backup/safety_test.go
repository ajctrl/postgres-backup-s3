package backup

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bartels/postgres-backup-s3/internal/storage"
)

type publishingStore struct {
	recordingStore
	before, after func()
	published     int
	uploads       int
	ifAbsent      bool
}

func (s *publishingStore) Upload(_ context.Context, _ string, body io.ReadCloser, options storage.UploadOptions) error {
	s.uploads++
	s.ifAbsent = options.IfAbsent
	defer body.Close()
	if _, err := io.Copy(io.Discard, body); err != nil {
		return err
	}
	if s.before != nil {
		s.before()
	}
	if err := options.BeforePublish(); err != nil {
		return err
	}
	s.published++
	if s.after != nil {
		s.after()
	}
	return nil
}

func TestBackupClockRollbackStopsPublicationAndRetention(t *testing.T) {
	for _, mode := range []string{"timestamp", "fixed"} {
		for _, phase := range []string{"before publication", "after publication", "no rollback"} {
			t.Run(mode+"/"+phase, func(t *testing.T) {
				dir := t.TempDir()
				writeFakeDump(t, dir, customArchiveFixture(16, tocFixture{kind: "DATABASE", section: 2, definition: "CREATE DATABASE app"}))
				t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
				started := time.Now()
				current := started
				store := &publishingStore{}
				store.objects = []storage.Object{{Key: "backup/v2/full/app/2000-01-01T00:00:00-0123456789abcdef0123456789abcdef.dump", LastModified: started.Add(-30 * 24 * time.Hour)}}
				rollback := func() { current = started.Add(-time.Nanosecond) }
				if phase == "before publication" {
					store.before = rollback
				} else if phase == "after publication" {
					store.after = rollback
				}
				var output bytes.Buffer
				a := New(Config{Databases: "app,billing", BackupAll: "false", Prefix: "backup", FilenameMode: mode, KeepDays: "7"}, store, &output, &output)
				a.lockPath = filepath.Join(dir, "lock")
				a.now = func() time.Time { return current }
				err := a.Backup(t.Context())
				if phase == "no rollback" {
					if err != nil || store.published != 2 || len(store.deleted) != 1 {
						t.Fatalf("equal clock should succeed: err=%v published=%d deleted=%v", err, store.published, store.deleted)
					}
				} else {
					if !errors.Is(err, errClockRollback) || store.uploads != 1 || store.prefix != "" || len(store.deleted) != 0 || strings.Contains(output.String(), "Backup complete") {
						t.Fatalf("unsafe rollback: err=%v uploads=%d listed=%q deleted=%v output=%s", err, store.uploads, store.prefix, store.deleted, &output)
					}
					if phase == "before publication" && store.published != 0 {
						t.Fatal("published despite rollback")
					}
				}
				if store.ifAbsent != (mode == "timestamp") {
					t.Fatalf("incorrect overwrite policy for %s", mode)
				}
			})
		}
	}
}

func writeFakeDump(t *testing.T, dir string, archive []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "psql"), []byte("#!/bin/sh\nprintf '{\"standby\":false,\"unlogged\":false}\\n'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "archive.dump"), archive, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "billing.dump"), bytes.Replace(archive, append(archiveInt(3, 4), []byte("app")...), append(archiveInt(7, 4), []byte("billing")...), 1), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "pg_dump"), []byte(`#!/bin/sh
file=archive.dump
for arg do case "$arg" in postgresql:///billing\?*) file=billing.dump;; esac; done
cat "${0%/*}/$file"
`), 0700); err != nil {
		t.Fatal(err)
	}
}

func TestAmbiguousBackupNeverPublishesOrRunsRetention(t *testing.T) {
	dir := t.TempDir()
	writeFakeDump(t, dir, customArchiveFixture(16,
		tocFixture{kind: "DATABASE", section: 2, definition: "CREATE DATABASE app", drop: "DROP DATABASE app"},
		tocFixture{kind: "BLOB METADATA", section: 3, definition: "90000\n", drop: "-- dummy"},
		tocFixture{kind: "BLOBS", section: 3}))
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	store := &publishingStore{}
	var output bytes.Buffer
	a := New(Config{Database: "app", BackupAll: "false", FilenameMode: "fixed", KeepDays: "7"}, store, &output, &output)
	a.lockPath = filepath.Join(dir, "lock")
	if err := a.Backup(t.Context()); err == nil || !strings.Contains(output.String(), "ambiguous archive") {
		t.Fatalf("ambiguous backup did not fail: %v\n%s", err, &output)
	}
	if store.published != 0 || store.prefix != "" || len(store.deleted) != 0 || strings.Contains(output.String(), "Backup complete") {
		t.Fatalf("failed backup was published or retention ran: %#v\n%s", store, &output)
	}
}

func TestRetentionClockRollbackAfterListingPreventsDeletion(t *testing.T) {
	started := time.Now()
	s := &recordingStore{objects: []storage.Object{{Key: "backup/v2/full/app/2000-01-01T00:00:00-0123456789abcdef0123456789abcdef.dump", LastModified: started.Add(-48 * time.Hour)}}}
	a := New(Config{Prefix: "backup"}, s, io.Discard, io.Discard)
	checks := 0
	err := a.removeOldBackups(t.Context(), "app", "new-key", 1, func() error {
		checks++
		if checks == 2 {
			return errClockRollback
		}
		return nil
	})
	if !errors.Is(err, errClockRollback) || len(s.deleted) != 0 || s.prefix == "" {
		t.Fatalf("err=%v deleted=%v prefix=%q", err, s.deleted, s.prefix)
	}
}

func TestLegacyTimestampKeyExcluded(t *testing.T) {
	c := Config{Prefix: "backup"}
	if c.isTimestampBackup("app", "backup/v2/full/app/2000-01-01T00:00:00.dump") {
		t.Fatal("legacy timestamp-only key was selected")
	}
}

type deadlineStore struct {
	publishingStore
	versioningTimedOut, retentionTimedOut bool
}

func (s *deadlineStore) VersioningEnabled(ctx context.Context) (bool, error) {
	if _, ok := ctx.Deadline(); !ok {
		return false, errors.New("missing versioning deadline")
	}
	<-ctx.Done()
	s.versioningTimedOut = errors.Is(ctx.Err(), context.DeadlineExceeded)
	return false, ctx.Err()
}

func (s *deadlineStore) ListPages(ctx context.Context, _ string, _ func(storage.Listing) error) error {
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("missing retention deadline")
	}
	<-ctx.Done()
	s.retentionTimedOut = errors.Is(ctx.Err(), context.DeadlineExceeded)
	return ctx.Err()
}

func TestVersioningAndRetentionHaveSeparateDeadlines(t *testing.T) {
	dir := t.TempDir()
	writeFakeDump(t, dir, customArchiveFixture(16, tocFixture{kind: "DATABASE", section: 2, definition: "CREATE DATABASE app"}))
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	s := &deadlineStore{}
	app := New(Config{Database: "app", BackupAll: "false", FilenameMode: "fixed", KeepDays: "7", DatabaseTimeout: 250 * time.Millisecond}, s, io.Discard, io.Discard)
	app.lockPath = filepath.Join(dir, "backup.lock")
	err := app.Backup(t.Context())
	if err == nil || !s.versioningTimedOut || !s.retentionTimedOut || s.published != 1 || !s.ifAbsent {
		t.Fatalf("expected timestamp fallback and bounded cleanup: published=%d versionTimeout=%v retentionTimeout=%v err=%v", s.published, s.versioningTimedOut, s.retentionTimedOut, err)
	}
}

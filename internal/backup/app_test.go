package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/bartels/postgres-backup-s3/internal/storage"
)

type recordingStore struct {
	objects    []storage.Object
	deleted    []string
	prefix     string
	listErr    error
	serverTime *time.Time
}

func (s *recordingStore) Modified(context.Context, string) (time.Time, bool, error) {
	return time.Time{}, false, nil
}
func (s *recordingStore) VersioningEnabled(context.Context) (bool, error) { return true, nil }
func (s *recordingStore) Upload(context.Context, string, io.ReadCloser, storage.UploadOptions) error {
	return nil
}
func (s *recordingStore) DownloadTo(context.Context, string, string, *os.File) error { return nil }
func (s *recordingStore) List(_ context.Context, prefix string) (storage.Listing, error) {
	s.prefix = prefix
	now := time.Now()
	if s.serverTime != nil {
		now = *s.serverTime
	}
	return storage.Listing{Objects: s.objects, ServerTime: now}, s.listErr
}
func (s *recordingStore) ListPages(ctx context.Context, prefix string, visit func(storage.Listing) error) error {
	page, err := s.List(ctx, prefix)
	if err != nil {
		return err
	}
	return visit(page)
}
func (s *recordingStore) DeleteBatch(ctx context.Context, keys []string, guard func() error) error {
	for _, key := range keys {
		if err := s.Delete(ctx, key, guard); err != nil {
			return err
		}
	}
	return nil
}
func (s *recordingStore) Delete(_ context.Context, key string, beforeDelete func() error) error {
	if beforeDelete != nil {
		if err := beforeDelete(); err != nil {
			return err
		}
	}
	s.deleted = append(s.deleted, key)
	return nil
}

func TestPlaintextRestoreFileHasNoDirectoryEntry(t *testing.T) {
	dir := t.TempDir()
	f, err := anonymousDump(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.WriteString("private archive contents"); err != nil {
		t.Fatal(err)
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 0 {
		t.Fatalf("plaintext has a directory entry: %v %v", files, err)
	}
}

func TestRetentionBoundaryAndExactMatching(t *testing.T) {
	cutoff := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	expired := []storage.Object{
		{Key: "backup/v2/full/app/2026-09-05T11:59:59-0123456789abcdef0123456789abcdef.dump", LastModified: cutoff.Add(-time.Second)},
		{Key: "backup/v2/full/app/2026-09-05T11:59:59-0123456789abcdef0123456789abcdef.dump.gpg", LastModified: cutoff.Add(-time.Nanosecond)},
	}
	objects := append([]storage.Object{}, expired...)
	objects = append(objects,
		storage.Object{Key: "backup/v2/full/app/2026-09-05T12:00:00-0123456789abcdef0123456789abcdef.dump", LastModified: cutoff},
		storage.Object{Key: "backup/v2/full/app/2026-09-05T12:00:01-0123456789abcdef0123456789abcdef.dump.gpg", LastModified: cutoff.Add(time.Nanosecond)},
	)
	for _, key := range []string{
		"backup/v2/full/app.dump", "backup/v2/full/app.dump.gpg", "backup/v2/full/app_extra/2000-01-01T00:00:00-0123456789abcdef0123456789abcdef.dump",
		"backup/v2/full/app/2000-01-01T00%3A00%3A00/latest.dump", "backup/v2/full/other/2000-01-01T00:00:00-0123456789abcdef0123456789abcdef.dump",
		"backup/v2/full/app/notes.dump", "backup/v2/full/app/latest.dump", "backup/v2/full/app_2000-01-01T00:00:00-0123456789abcdef0123456789abcdef.dump", "backup/v2/full/app/2000-01-01T00:00:00-0123456789abcdef0123456789abcdef.dump.extra", "backup/v2/full/app/2000-01-01T00:00:00-0123456789abcdef0123456789abcdef.dump\n",
	} {
		objects = append(objects, storage.Object{Key: key, LastModified: cutoff.Add(-time.Hour)})
	}
	for _, mode := range []string{"timestamp", "fixed"} {
		serverTime := cutoff.Add(24 * time.Hour)
		s := &recordingStore{objects: objects, serverTime: &serverTime}
		a := New(Config{Prefix: "backup", FilenameMode: mode}, s, io.Discard, io.Discard)
		if err := a.removeOldBackups(context.Background(), "app", "new-key", 1, func() error { return nil }); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(s.deleted, []string{expired[0].Key, expired[1].Key}) || s.prefix != "backup/v2/full/app/" {
			t.Errorf("mode %s: deleted %v, prefix %q", mode, s.deleted, s.prefix)
		}
	}
}

func TestRetentionNeverDeletesAfterIncompleteListing(t *testing.T) {
	for _, s := range []*recordingStore{
		{objects: []storage.Object{{Key: "backup/v2/full/app/2000-01-01T00:00:00-0123456789abcdef0123456789abcdef.dump"}}},
		{listErr: errors.New("second page failed")},
	} {
		a := New(Config{Prefix: "backup"}, s, io.Discard, io.Discard)
		if err := a.removeOldBackups(context.Background(), "app", "new-key", 1, func() error { return nil }); err == nil || len(s.deleted) != 0 {
			t.Errorf("unsafe incomplete listing: err=%v deleted=%v", err, s.deleted)
		}
	}
}

func TestBackupLockAndCanceledRun(t *testing.T) {
	path := filepath.Join(t.TempDir(), "backup.lock")
	lock, err := acquireLock(path)
	if err != nil {
		t.Fatal(err)
	}
	if second, err := acquireLock(path); err == nil {
		second.Close()
		t.Fatal("overlapping lock accepted")
	}
	lock.Close()
	second, err := acquireLock(path)
	if err != nil {
		t.Fatal(err)
	}
	second.Close()
	a := New(Config{}, &recordingStore{}, io.Discard, io.Discard)
	a.lockPath = path
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := a.Backup(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled backup = %v", err)
	}
}

func TestLargeRetentionBatches(t *testing.T) {
	var q expiredKeys
	defer q.close()
	var want, got []string
	for i := 0; i < 2043; i++ {
		key := fmt.Sprintf("multi\nline-%d", i)
		want = append(want, key)
		if err := q.add(key); err != nil {
			t.Fatal(err)
		}
	}
	var sizes []int
	err := q.batches(func(keys []string) error {
		sizes = append(sizes, len(keys))
		got = append(got, keys...)
		return nil
	})
	if err != nil || !slices.Equal(sizes, []int{1000, 1000, 43}) || !slices.Equal(want, got) {
		t.Fatalf("retention spool mismatch: sizes=%v err=%v", sizes, err)
	}
}

type blockedDownload struct{ recordingStore }

func (*blockedDownload) DownloadTo(ctx context.Context, key, versionID string, dst *os.File) error {
	<-ctx.Done()
	return ctx.Err()
}

func TestRestoreDeadlineReleasesLock(t *testing.T) {
	app := New(Config{Database: "app", FilenameMode: "fixed", RestoreTimeout: 25 * time.Millisecond}, &blockedDownload{}, io.Discard, io.Discard)
	app.lockPath = filepath.Join(t.TempDir(), "backup.lock")
	if err := app.Restore(t.Context(), nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("restore deadline: %v", err)
	}
	lock, err := acquireLock(app.lockPath)
	if err != nil {
		t.Fatal(err)
	}
	lock.Close()
}

func TestLockRejectsSpecialFiles(t *testing.T) {
	dir := t.TempDir()
	fifo := filepath.Join(dir, "fifo")
	if err := syscall.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(filepath.Join(dir, "other"), link); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{fifo, link} {
		if lock, err := acquireLock(path); err == nil {
			lock.Close()
			t.Fatalf("accepted special file %q", path)
		}
	}
}

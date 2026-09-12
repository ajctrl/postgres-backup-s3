package backup

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/bartels/postgres-backup-s3/internal/storage"
)

type recordingStore struct {
	objects []storage.Object
	deleted []string
	prefix  string
	listErr error
}

func (s *recordingStore) VersioningEnabled(context.Context) (bool, error)        { return true, nil }
func (s *recordingStore) Upload(context.Context, string, io.ReadCloser) error    { return nil }
func (s *recordingStore) Download(context.Context, string, string, string) error { return nil }
func (s *recordingStore) List(_ context.Context, prefix string) ([]storage.Object, error) {
	s.prefix = prefix
	return s.objects, s.listErr
}
func (s *recordingStore) Delete(_ context.Context, key string) error {
	s.deleted = append(s.deleted, key)
	return nil
}

func TestRetentionBoundaryAndExactMatching(t *testing.T) {
	cutoff := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	expired := []storage.Object{
		{Key: "backup/app_2026-09-05T11:59:59.dump", LastModified: cutoff.Add(-time.Second)},
		{Key: "backup/app_2026-09-05T11:59:59.dump.gpg", LastModified: cutoff.Add(-time.Nanosecond)},
	}
	objects := append([]storage.Object{}, expired...)
	objects = append(objects,
		storage.Object{Key: "backup/app_2026-09-05T12:00:00.dump", LastModified: cutoff},
		storage.Object{Key: "backup/app_2026-09-05T12:00:01.dump.gpg", LastModified: cutoff.Add(time.Nanosecond)},
	)
	for _, key := range []string{
		"backup/app/latest.dump", "backup/app/latest.dump.gpg", "backup/app_extra_2000-01-01T00:00:00.dump",
		"backup/app_2000-01-01T00%3A00%3A00/latest.dump", "backup/other_2000-01-01T00:00:00.dump",
		"backup/app_notes.dump", "backup/app_2000-01-01T00:00:00.dump.extra", "backup/app_2000-01-01T00:00:00.dump\n",
	} {
		objects = append(objects, storage.Object{Key: key, LastModified: cutoff.Add(-time.Hour)})
	}
	for _, mode := range []string{"timestamp", "fixed"} {
		s := &recordingStore{objects: objects}
		a := New(Config{Prefix: "backup", FilenameMode: mode}, s, io.Discard, io.Discard)
		if err := a.removeOldBackups(context.Background(), "app", cutoff); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(s.deleted, []string{expired[0].Key, expired[1].Key}) || s.prefix != "backup/app_" {
			t.Errorf("mode %s: deleted %v, prefix %q", mode, s.deleted, s.prefix)
		}
	}
}

func TestRetentionNeverDeletesAfterIncompleteListing(t *testing.T) {
	for _, s := range []*recordingStore{
		{objects: []storage.Object{{Key: "backup/app_2000-01-01T00:00:00.dump"}}},
		{listErr: errors.New("second page failed")},
	} {
		a := New(Config{Prefix: "backup"}, s, io.Discard, io.Discard)
		if err := a.removeOldBackups(context.Background(), "app", time.Now()); err == nil || len(s.deleted) != 0 {
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

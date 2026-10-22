package backup

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bartels/postgres-backup-s3/internal/storage"
)

type retentionProbeStore struct {
	publishingStore
	key        string
	uploadTime time.Time
}

func (s *retentionProbeStore) Upload(ctx context.Context, key string, body io.ReadCloser, options storage.UploadOptions) error {
	if err := s.publishingStore.Upload(ctx, key, body, options); err != nil {
		return err
	}
	s.key = key
	s.objects = append(s.objects, storage.Object{Key: key, LastModified: s.uploadTime})
	return nil
}

func TestRetentionUsesServerClockAndProtectsPublishedKey(t *testing.T) {
	serverTime := time.Date(2026, 9, 12, 12, 0, 0, 0, time.UTC)
	for _, mode := range []string{"timestamp", "fixed"} {
		for _, scenario := range []string{"host ahead", "host behind", "missing date", "published key appears old"} {
			t.Run(mode+"/"+scenario, func(t *testing.T) {
				dir := t.TempDir()
				writeFakeDump(t, dir, customArchiveFixture(16, tocFixture{kind: "DATABASE", section: 2, definition: "CREATE DATABASE app"}))
				t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
				date := serverTime
				s := &retentionProbeStore{uploadTime: serverTime}
				s.serverTime = &date
				old := "backup/app/2000-01-01T00:00:00-0123456789abcdef0123456789abcdef.dump"
				recent := "backup/app/2000-01-02T00:00:00-0123456789abcdef0123456789abcdef.dump"
				s.objects = []storage.Object{{Key: old, LastModified: serverTime.Add(-8 * 24 * time.Hour)}, {Key: recent, LastModified: serverTime.Add(-6 * 24 * time.Hour)}}
				localTime := serverTime.Add(30 * 24 * time.Hour)
				if scenario == "host behind" {
					localTime = serverTime.Add(-30 * 24 * time.Hour)
				}
				if scenario == "missing date" {
					date = time.Time{}
				}
				if scenario == "published key appears old" {
					s.uploadTime = serverTime.Add(-30 * 24 * time.Hour)
				}
				a := New(Config{Database: "app", BackupAll: "false", Prefix: "backup", FilenameMode: mode, KeepDays: "7"}, s, io.Discard, io.Discard)
				a.lockPath = filepath.Join(dir, "lock")
				a.now = func() time.Time { return localTime }
				err := a.Backup(t.Context())
				if s.published != 1 {
					t.Fatalf("backup not retained: err=%v", err)
				}
				if scenario == "missing date" {
					if err == nil || len(s.deleted) != 0 {
						t.Fatalf("missing Date: err=%v deleted=%v", err, s.deleted)
					}
				} else if err != nil || len(s.deleted) != 1 || s.deleted[0] != old {
					t.Fatalf("err=%v deleted=%v new=%s", err, s.deleted, s.key)
				}
			})
		}
	}
}

func TestBackupSourcePolicy(t *testing.T) {
	for _, tc := range []struct {
		name, state                    string
		partial, success, lateUnlogged bool
	}{
		{"primary", `{"standby":false,"unlogged":true}`, false, true, false},
		{"standby logged", `{"standby":true,"unlogged":false}`, false, true, false},
		{"standby unlogged", `{"standby":true,"unlogged":true}`, false, false, false},
		{"standby opt in", `{"standby":true,"unlogged":true}`, true, true, false},
		{"unknown", `{}`, false, false, false},
		{"null", `null`, true, false, false},
		{"invalid", `unexpected`, true, false, false},
		{"intervening DDL", `{"standby":true,"unlogged":false}`, false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			definition := "CREATE TABLE public.probe (id integer);"
			if tc.lateUnlogged {
				definition = "CREATE UNLOGGED TABLE public.probe (id integer);"
			}
			writeFakeDump(t, dir, customArchiveFixture(16, tocFixture{kind: "TABLE", section: 2, name: "probe", definition: definition, drop: "DROP TABLE public.probe"}))
			t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
			t.Setenv("TEST_SOURCE_STATE", tc.state)
			if err := os.WriteFile(filepath.Join(dir, "psql"), []byte("#!/bin/sh\nprintf '%s\\n' \"$TEST_SOURCE_STATE\"\n"), 0700); err != nil {
				t.Fatal(err)
			}
			var log bytes.Buffer
			s := &publishingStore{}
			a := New(Config{Database: "app", BackupAll: "false", FilenameMode: "timestamp", AllowPartial: tc.partial, KeepDays: "7"}, s, &log, &log)
			a.lockPath = filepath.Join(dir, "lock")
			err := a.Backup(t.Context())
			if (err == nil) != tc.success {
				t.Fatalf("err=%v log=%s", err, &log)
			}
			if !tc.success && (s.published != 0 || s.prefix != "") {
				t.Fatalf("failed source published/ran retention: %#v", s)
			}
			if tc.partial && tc.success && !strings.Contains(log.String(), "WARNING: standby backup") {
				t.Fatal("missing omission warning")
			}
		})
	}
}

func TestDiscoveryForcesUTF8AndRejectsInvalidBytes(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		dir := t.TempDir()
		body := []byte("[\"café\"]")
		if invalid {
			body = []byte{'[', '"', 'c', 'a', 'f', 0xe9, '"', ']'}
		}
		if err := os.WriteFile(filepath.Join(dir, "names"), body, 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "psql"), []byte("#!/bin/sh\n[ \"$PGCLIENTENCODING\" = UTF8 ] || exit 8\ncat \"${0%/*}/names\"\n"), 0700); err != nil {
			t.Fatal(err)
		}
		t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
		t.Setenv("PGCLIENTENCODING", "LATIN1")
		a := New(Config{BackupAll: "true"}, nil, io.Discard, io.Discard)
		names, err := a.databases(t.Context(), commandRunner{stderr: io.Discard})
		if invalid {
			if err == nil || !strings.Contains(err.Error(), "invalid UTF-8") {
				t.Fatalf("invalid bytes accepted: %q %v", names, err)
			}
		} else if err != nil || len(names) != 1 || names[0] != "café" {
			t.Fatalf("name changed: %q %v", names, err)
		}
	}
}

func TestRestoreDiagnosticsNeverExposeSQLOrSecrets(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "pg_restore"), []byte("#!/bin/sh\nprintf 'password synthetic-secret\\n'\nprintf 'Command was: secret SQL\\nDETAIL: synthetic-secret\\n' >&2\nexit 1\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
	var log bytes.Buffer
	err := (commandRunner{stderr: &log}).restore(t.Context())
	if err == nil || !strings.Contains(err.Error(), "pg_restore failed") || strings.Contains(err.Error(), "synthetic-secret") || log.Len() != 0 {
		t.Fatalf("unsafe diagnostics: %v %s", err, &log)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := (commandRunner{stderr: &log}).restore(ctx); err == nil {
		t.Fatal("cancellation reported success")
	}
}

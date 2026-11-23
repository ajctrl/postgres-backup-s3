package backup

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bartels/postgres-backup-s3/internal/storage"
)

func TestRetentionStopsRetryAfterClockRollback(t *testing.T) {
	dir := t.TempDir()
	writeFakeDump(t, dir, customArchiveFixture(16, tocFixture{kind: "DATABASE", section: 2, definition: "CREATE DATABASE app"}))
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("AWS_MAX_ATTEMPTS", "2")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	var rolledBack, deleted atomic.Bool
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		switch r.Method {
		case "PUT":
			w.Header().Set("ETag", `"new"`)
		case "GET":
			fmt.Fprint(w, `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>backup/v2/full/app/2000-01-01T00:00:00-0123456789abcdef0123456789abcdef.dump</Key><LastModified>2000-01-01T00:00:00Z</LastModified><Size>100</Size></Contents></ListBucketResult>`)
		case "DELETE":
			if attempts.Add(1) == 1 {
				rolledBack.Store(true)
				w.WriteHeader(http.StatusServiceUnavailable)
				fmt.Fprint(w, `<Error><Code>ServiceUnavailable</Code></Error>`)
				return
			}
			deleted.Store(true)
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected method: %s", r.Method)
		}
	}))
	defer server.Close()
	s, err := storage.New(t.Context(), storage.Config{Bucket: "test", Region: "us-east-1", Endpoint: server.URL, AccessKeyID: "test", SecretAccessKey: "test"})
	if err != nil {
		t.Fatal(err)
	}
	var log bytes.Buffer
	a := New(Config{Database: "app", BackupAll: "false", Prefix: "backup", FilenameMode: "timestamp", KeepDays: "7"}, s, &log, &log)
	a.lockPath = filepath.Join(dir, "lock")
	started := time.Now()
	a.now = func() time.Time {
		if rolledBack.Load() {
			return started.Add(-time.Second)
		}
		return started
	}
	err = a.Backup(t.Context())
	if !errors.Is(err, errClockRollback) || attempts.Load() != 1 || deleted.Load() {
		t.Fatalf("err=%v attempts=%d deleted=%v\n%s", err, attempts.Load(), deleted.Load(), &log)
	}
}

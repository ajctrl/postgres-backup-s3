package storage

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestAttemptGuardChecksRetriesAndAllowsAbort(t *testing.T) {
	for _, operation := range []string{"put", "complete", "delete"} {
		for _, rollback := range []bool{false, true} {
			name := operation + "/normal retry"
			if rollback {
				name = operation + "/rollback before retry"
			}
			t.Run(name, func(t *testing.T) {
				var rolledBack, published, aborted atomic.Bool
				var attempts, checks atomic.Int32
				s := testS3(t, func(w http.ResponseWriter, r *http.Request) {
					io.Copy(io.Discard, r.Body)
					switch {
					case r.URL.Query().Has("uploads"):
						writeXML(w, `<InitiateMultipartUploadResult><UploadId>guard-retry</UploadId></InitiateMultipartUploadResult>`)
						return
					case r.URL.Query().Has("partNumber"):
						w.Header().Set("ETag", `"part"`)
						return
					case r.Method == "DELETE" && r.URL.Query().Has("uploadId"):
						aborted.Store(true)
						w.WriteHeader(http.StatusNoContent)
						return
					}
					if attempts.Add(1) == 1 {
						rolledBack.Store(rollback)
						writeS3Error(w, http.StatusServiceUnavailable, "ServiceUnavailable")
						return
					}
					published.Store(true)
					if operation == "complete" {
						writeXML(w, `<CompleteMultipartUploadResult><ETag>"complete"</ETag></CompleteMultipartUploadResult>`)
					} else if operation == "delete" {
						w.WriteHeader(http.StatusNoContent)
					} else {
						w.Header().Set("ETag", `"complete"`)
					}
				})
				s.client = s3.New(s.client.Options(), func(o *s3.Options) {
					o.RetryMaxAttempts = 3
					o.Retryer = retry.NewStandard(func(o *retry.StandardOptions) {
						o.MaxAttempts = 3
						o.Backoff = retry.BackoffDelayerFunc(func(int, error) (time.Duration, error) { return 0, nil })
					})
				})
				guardErr := errors.New("clock moved backwards")
				guard := func() error {
					checks.Add(1)
					if rolledBack.Load() {
						return guardErr
					}
					return nil
				}
				var err error
				if operation == "delete" {
					err = s.Delete(t.Context(), "old.dump", guard)
				} else {
					size := 100
					if operation == "complete" {
						size = 6 * 1024 * 1024
					}
					s.uploadPartSize = 5 * 1024 * 1024
					err = s.Upload(t.Context(), "new.dump", io.NopCloser(bytes.NewReader(make([]byte, size))), UploadOptions{IfAbsent: true, BeforePublish: guard})
				}
				wantAttempts := int32(2)
				if rollback {
					wantAttempts = 1
				}
				if checks.Load() != 2 || attempts.Load() != wantAttempts || published.Load() == rollback {
					t.Fatalf("checks=%d attempts=%d published=%v err=%v", checks.Load(), attempts.Load(), published.Load(), err)
				}
				if rollback && !errors.Is(err, guardErr) || !rollback && err != nil {
					t.Fatalf("guard error not preserved: %v", err)
				}
				if aborted.Load() != (operation == "complete" && rollback) {
					t.Fatalf("incorrect multipart cleanup: aborted=%v", aborted.Load())
				}
			})
		}
	}
}

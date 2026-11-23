package storage

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"strings"
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
				enablePublicationRetries(s)
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

func enablePublicationRetries(s *S3) {
	s.client = s3.New(s.client.Options(), func(o *s3.Options) {
		o.RetryMaxAttempts = 3
		o.Retryer = retry.NewStandard(func(o *retry.StandardOptions) {
			o.MaxAttempts = 3
			o.Backoff = retry.BackoffDelayerFunc(func(int, error) (time.Duration, error) { return 0, nil })
		})
	})
}

func TestAmbiguousPublication(t *testing.T) {
	for _, mode := range []string{"timestamp", "fixed", "multipart", "other publication"} {
		t.Run(mode, func(t *testing.T) {
			var id atomic.Value
			var attempts, heads atomic.Int32
			const header = "X-Amz-Meta-Publication-Id"
			s := testS3(t, func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				if value := r.Header.Get(header); value != "" {
					id.Store(value)
				}
				switch {
				case r.Method == http.MethodHead:
					heads.Add(1)
					value, ok := id.Load().(string)
					if !ok || len(value) != 32 {
						t.Error("publication request missing metadata")
					}
					if mode == "other publication" {
						value = "different"
					}
					w.Header().Set(header, value)
				case r.URL.Query().Has("uploads"):
					writeXML(w, "<InitiateMultipartUploadResult><UploadId>lost-ack</UploadId></InitiateMultipartUploadResult>")
				case r.URL.Query().Has("partNumber"):
					w.Header().Set("ETag", "\"part\"")
				default:
					if attempts.Add(1) == 1 {
						// The first request committed. Its success response was lost.
						writeS3Error(w, http.StatusBadGateway, "InternalError")
					} else if mode == "multipart" {
						writeS3Error(w, http.StatusNotFound, "NoSuchUpload")
					} else {
						writeS3Error(w, http.StatusPreconditionFailed, "PreconditionFailed")
					}
				}
			})
			enablePublicationRetries(s)
			s.uploadPartSize = 5 << 20
			size := 50
			if mode == "multipart" {
				size = 6 << 20
			}
			err := s.Upload(t.Context(), "app.dump", io.NopCloser(bytes.NewReader(make([]byte, size))), UploadOptions{IfAbsent: mode != "fixed"})
			wantAttempts := int32(2)
			if mode == "fixed" {
				wantAttempts = 1
			}
			if (err != nil) != (mode == "other publication") || attempts.Load() != wantAttempts || heads.Load() != 1 {
				t.Fatalf("attempts=%d heads=%d err=%v", attempts.Load(), heads.Load(), err)
			}
		})
	}
}

func TestBulkDeleteReportsPerKeyFailure(t *testing.T) {
	s := testS3(t, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		sum := md5.Sum(body)
		if err != nil || r.Header.Get("Content-MD5") != base64.StdEncoding.EncodeToString(sum[:]) || !strings.Contains(r.Header.Get("Authorization"), "content-md5") {
			t.Error("bulk delete body MD5 missing or unsigned")
		}
		if !r.URL.Query().Has("delete") {
			t.Error("expected multi-object delete")
		}
		writeXML(w, "<DeleteResult><Error><Key>b</Key><Code>AccessDenied</Code></Error></DeleteResult>")
	})
	err := s.DeleteBatch(t.Context(), []string{"a", "b"}, nil)
	if err == nil || !strings.Contains(err.Error(), "AccessDenied") {
		t.Fatalf("expected individual deletion failure: %v", err)
	}
}

func TestDeleteKeysOutsideXMLCharacterSet(t *testing.T) {
	seen := make(chan string, 2)
	s := testS3(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("unexpected method: %s", r.Method)
		}
		seen <- r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	})
	if err := s.DeleteBatch(t.Context(), []string{"key\x01", "key\uffff"}, nil); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2 {
		t.Fatal("missing deletes")
	}
	if a, b := <-seen, <-seen; a != "/test-bucket/key\x01" || b != "/test-bucket/key\uffff" {
		t.Fatalf("keys changed: %q %q", a, b)
	}
}

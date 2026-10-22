package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

func TestUploadPublicationProtection(t *testing.T) {
	guardErr := errors.New("clock rollback")
	for _, size := range []int{100, 6 * 1024 * 1024} {
		for _, mode := range []string{"guard rejection", "collision", "new generation", "fixed version"} {
			t.Run(fmt.Sprintf("%d/%s", size, mode), func(t *testing.T) {
				var initiated, aborted, published atomic.Bool
				ifAbsent := mode != "fixed version"
				s := testS3(t, func(w http.ResponseWriter, r *http.Request) {
					switch {
					case r.Method == http.MethodPost && r.URL.Query().Has("uploads"):
						initiated.Store(true)
						writeXML(w, `<InitiateMultipartUploadResult><UploadId>guard</UploadId></InitiateMultipartUploadResult>`)
					case r.Method == http.MethodPut && r.URL.Query().Has("partNumber"):
						io.Copy(io.Discard, r.Body)
						w.Header().Set("ETag", `"part"`)
					case r.Method == http.MethodDelete:
						aborted.Store(true)
						w.WriteHeader(http.StatusNoContent)
					default:
						if mode == "guard rejection" {
							t.Error("publication request escaped the clock guard")
						}
						want := ""
						if ifAbsent {
							want = "*"
						}
						if got := r.Header.Get("If-None-Match"); got != want {
							t.Errorf("If-None-Match = %q, want %q", got, want)
						}
						io.Copy(io.Discard, r.Body)
						if mode == "collision" {
							writeS3Error(w, http.StatusPreconditionFailed, "PreconditionFailed")
							return
						}
						published.Store(true)
						if r.Method == http.MethodPost {
							writeXML(w, `<CompleteMultipartUploadResult><ETag>"complete"</ETag></CompleteMultipartUploadResult>`)
						} else {
							w.Header().Set("ETag", `"complete"`)
						}
					}
				})
				s.uploadPartSize = 5 * 1024 * 1024
				checks := 0
				err := s.Upload(t.Context(), "key.dump", io.NopCloser(bytes.NewReader(make([]byte, size))), UploadOptions{
					IfAbsent: ifAbsent,
					BeforePublish: func() error {
						checks++
						if mode == "guard rejection" {
							return guardErr
						}
						return nil
					},
				})
				wantSuccess := mode == "new generation" || mode == "fixed version"
				if (err == nil) != wantSuccess || published.Load() != wantSuccess || checks != 1 {
					t.Fatalf("err=%v published=%v checks=%d", err, published.Load(), checks)
				}
				if mode == "guard rejection" && !errors.Is(err, guardErr) {
					t.Fatalf("guard error lost: %v", err)
				}
				if mode == "collision" && !strings.Contains(err.Error(), "PreconditionFailed") {
					t.Fatalf("collision error lost: %v", err)
				}
				if initiated.Load() != (size > 5*1024*1024) || aborted.Load() != (initiated.Load() && !wantSuccess) {
					t.Fatalf("initiated=%v aborted=%v", initiated.Load(), aborted.Load())
				}
			})
		}
	}
}

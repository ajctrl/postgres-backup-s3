package storage

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestFixedReferenceFailurePreservesGenerations(t *testing.T) {
	const key = "backup/v2/full/app.dump"
	const prefix = key + ".generations/"
	var mu sync.Mutex
	objects := make(map[string]string)
	var rejectReference atomic.Bool
	s := testS3(t, func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/test-bucket/")
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodPut:
			if name == key && rejectReference.Load() {
				writeS3Error(w, http.StatusForbidden, "AccessDenied")
				return
			}
			if name != key && (!strings.HasPrefix(name, prefix) || r.Header.Get("If-None-Match") != "*") {
				t.Errorf("unexpected generation PUT %q, If-None-Match=%q", name, r.Header.Get("If-None-Match"))
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			objects[name] = string(body)
			w.Header().Set("ETag", `"stored"`)
		case http.MethodGet:
			body, ok := objects[name]
			if !ok {
				writeS3Error(w, http.StatusNotFound, "NoSuchKey")
				return
			}
			w.Header().Set("ETag", `"stored"`)
			fmt.Fprint(w, body)
		default:
			t.Errorf("unexpected %s %q", r.Method, name)
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	upload := func(payload string) error {
		return s.Upload(t.Context(), key, io.NopCloser(strings.NewReader(payload)), UploadOptions{FixedReference: true})
	}
	for _, payload := range []string{"first archive", "second archive"} {
		if err := upload(payload); err != nil {
			t.Fatal(err)
		}
	}
	rejectReference.Store(true)
	err := upload("third archive")
	if err == nil || !strings.Contains(err.Error(), "AccessDenied") {
		t.Fatalf("expected reference publication failure, got %v", err)
	}
	path := filepath.Join(t.TempDir(), "restored.dump")
	if err := s.Download(t.Context(), key, "", path); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != "second archive" {
		t.Fatalf("restored %q, err=%v", got, err)
	}
	mu.Lock()
	defer mu.Unlock()
	saved := map[string]int{}
	for name, payload := range objects {
		if strings.HasPrefix(name, prefix) {
			saved[payload]++
			if payload == "third archive" && !strings.Contains(err.Error(), name) {
				t.Errorf("publication error omitted saved generation %q", name)
			}
		}
	}
	for _, payload := range []string{"first archive", "second archive", "third archive"} {
		if saved[payload] != 1 {
			t.Errorf("found %d generations containing %q", saved[payload], payload)
		}
	}
}

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

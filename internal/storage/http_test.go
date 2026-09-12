package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
)

func timeoutS3(t *testing.T, timeout time.Duration, handler http.HandlerFunc) *S3 {
	t.Helper()
	isolatedAWS(t)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	s, err := newS3(t.Context(), Config{
		Bucket: "test", Region: "us-west-1", Endpoint: server.URL,
		AccessKeyID: "test-key", SecretAccessKey: "test-secret",
	}, timeout)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func requireTimeout(t *testing.T, err error) {
	t.Helper()
	var timeout interface{ Timeout() bool }
	if !errors.As(err, &timeout) || !timeout.Timeout() {
		t.Fatalf("expected a network timeout, got %v", err)
	}
}

func TestS3StalledResponseTimesOutAndNextRequestSucceeds(t *testing.T) {
	var calls atomic.Int32
	s := timeoutS3(t, 250*time.Millisecond, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			<-r.Context().Done()
			return
		}
		writeXML(w, `<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`)
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err := s.VersioningEnabled(ctx)
	requireTimeout(t, err)
	if ctx.Err() != nil {
		t.Fatal("request only stopped because the test context expired")
	}
	if enabled, err := s.VersioningEnabled(ctx); err != nil || !enabled {
		t.Fatalf("request after timeout: enabled=%v err=%v", enabled, err)
	}
}

func TestS3StalledDownloadTimesOutWithoutReplacingDestination(t *testing.T) {
	s := timeoutS3(t, 250*time.Millisecond, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1000")
		w.Header().Set("ETag", `"test"`)
		w.Write([]byte("PGDMP"))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	})
	dir := t.TempDir()
	path := filepath.Join(dir, "db.dump")
	if err := os.WriteFile(path, []byte("existing"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	// The transfer manager includes the part-read error as text rather than
	// preserving its net.Error type. Verify this is the socket timeout, not
	// cancellation by the test's outer deadline.
	if err := s.Download(ctx, "key", "", path); err == nil || !strings.Contains(err.Error(), "i/o timeout") || ctx.Err() != nil {
		t.Fatalf("stalled download: err=%v context=%v", err, ctx.Err())
	}
	contents, err := os.ReadFile(path)
	entries, _ := os.ReadDir(dir)
	if err != nil || string(contents) != "existing" || len(entries) != 1 {
		t.Fatalf("download timeout changed destination or leaked files: %q, %v, %v", contents, entries, err)
	}
}

func TestS3ProgressingDownloadOutlastsIdleTimeout(t *testing.T) {
	const count = 20
	s := timeoutS3(t, time.Second, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(count))
		w.Header().Set("ETag", `"test"`)
		for i := 0; i < count; i++ {
			if _, err := w.Write([]byte("x")); err != nil {
				return
			}
			w.(http.Flusher).Flush()
			select {
			case <-r.Context().Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	path := filepath.Join(t.TempDir(), "db.dump")
	start := time.Now()
	if err := s.Download(ctx, "key", "", path); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(path)
	if err != nil || string(contents) != strings.Repeat("x", count) || time.Since(start) < time.Second {
		t.Fatalf("progressing download: bytes=%d duration=%v err=%v", len(contents), time.Since(start), err)
	}
}

type pacedBody struct {
	ctx       context.Context
	remaining int
}

func (b *pacedBody) Read(p []byte) (int, error) {
	if b.remaining == 0 {
		return 0, io.EOF
	}
	select {
	case <-b.ctx.Done():
		return 0, b.ctx.Err()
	case <-time.After(100 * time.Millisecond):
	}
	n := min(len(p), b.remaining, 1024)
	copy(p, bytes.Repeat([]byte("x"), n))
	b.remaining -= n
	return n, nil
}

func TestHTTPProgressingUploadOutlastsIdleTimeout(t *testing.T) {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor != 1 {
			t.Errorf("expected isolated HTTP/1 requests, got %s", r.Proto)
		}
		if n, err := io.Copy(io.Discard, r.Body); err != nil || n != 20*1024 {
			t.Errorf("upload: bytes=%d err=%v", n, err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()
	// Keep the custom trust roots while forcing HTTP/1 and rolling deadlines.
	base := awshttp.NewBuildableClient().WithTransportOptions(func(tr *http.Transport) {
		tr.TLSClientConfig = server.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	})
	client := withIdleTimeout(base, time.Second)
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, server.URL, &pacedBody{ctx: ctx, remaining: 20 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	req.ContentLength = 20 * 1024
	start := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("progressing upload timed out: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || time.Since(start) < time.Second {
		t.Fatalf("response=%s duration=%v", resp.Status, time.Since(start))
	}
}

func TestS3StalledMultipartUploadIsAborted(t *testing.T) {
	var aborted atomic.Bool
	s := timeoutS3(t, 250*time.Millisecond, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Query().Has("uploads"):
			writeXML(w, `<InitiateMultipartUploadResult><UploadId>stalled</UploadId></InitiateMultipartUploadResult>`)
		case r.Method == http.MethodPut:
			io.Copy(io.Discard, r.Body)
			<-r.Context().Done()
		case r.Method == http.MethodDelete:
			aborted.Store(true)
			w.WriteHeader(http.StatusNoContent)
		default:
			writeS3Error(w, http.StatusBadRequest, "BadRequest")
		}
	})
	path := filepath.Join(t.TempDir(), "large.dump")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(20 * 1024 * 1024); err != nil {
		f.Close()
		t.Fatal(err)
	}
	f.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := s.Upload(ctx, "large.dump", path); err == nil || ctx.Err() != nil {
		t.Fatalf("stalled multipart upload: err=%v context=%v", err, ctx.Err())
	}
	if !aborted.Load() {
		t.Fatal("timed out multipart upload was not aborted")
	}
}

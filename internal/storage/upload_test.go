package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type failingStream struct {
	io.Reader
	err error
}

func (r failingStream) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err == io.EOF {
		err = r.err
	}
	return n, err
}

func TestStreamingUploadRejectsProducerFailure(t *testing.T) {
	for _, size := range []int{100, 12 * 1024 * 1024} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			var initiated, aborted, published atomic.Bool
			s := testS3(t, func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodPost && r.URL.Query().Has("uploads"):
					initiated.Store(true)
					writeXML(w, `<InitiateMultipartUploadResult><UploadId>stream</UploadId></InitiateMultipartUploadResult>`)
				case r.Method == http.MethodPut && r.URL.Query().Has("partNumber"):
					io.Copy(io.Discard, r.Body)
					w.Header().Set("ETag", `"part"`)
				case r.Method == http.MethodDelete:
					aborted.Store(true)
					w.WriteHeader(http.StatusNoContent)
				default:
					published.Store(true)
					writeS3Error(w, http.StatusBadRequest, "UnexpectedPublication")
				}
			})
			sourceErr := errors.New("pg_dump failed after emitting data")
			body := io.NopCloser(failingStream{Reader: bytes.NewReader(make([]byte, size)), err: sourceErr})
			if err := s.Upload(t.Context(), "latest.dump", body); !errors.Is(err, sourceErr) {
				t.Fatalf("producer failure was lost: %v", err)
			}
			if published.Load() || initiated.Load() != aborted.Load() || initiated.Load() != (size > 8*1024*1024) {
				t.Fatalf("initiated=%v aborted=%v published=%v", initiated.Load(), aborted.Load(), published.Load())
			}
		})
	}
}

func TestStreamingUploadPartFailureInterruptsBlockedProducer(t *testing.T) {
	var aborted atomic.Bool
	s := testS3(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Query().Has("uploads"):
			writeXML(w, `<InitiateMultipartUploadResult><UploadId>stream</UploadId></InitiateMultipartUploadResult>`)
		case r.Method == http.MethodPut:
			io.Copy(io.Discard, r.Body)
			writeS3Error(w, http.StatusForbidden, "AccessDenied")
		case r.Method == http.MethodDelete:
			aborted.Store(true)
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
		}
	})
	reader, writer := io.Pipe()
	defer writer.Close()
	written := make(chan struct{})
	go func() {
		defer close(written)
		writer.Write(make([]byte, 8*1024*1024))
		// Leave the source open: the next read can finish only by cancellation.
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	err := s.Upload(ctx, "latest.dump", reader)
	<-written
	if err == nil || !strings.Contains(err.Error(), "AccessDenied") || ctx.Err() != nil || !aborted.Load() {
		t.Fatalf("err=%v outer context=%v aborted=%v", err, ctx.Err(), aborted.Load())
	}
}

func TestStreamingUploadCancellationInterruptsInitialRead(t *testing.T) {
	s := testS3(t, func(w http.ResponseWriter, r *http.Request) { t.Error("empty stalled source accessed S3") })
	reader, writer := io.Pipe()
	defer writer.Close()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- s.Upload(ctx, "key", reader) }()
	if _, err := writer.Write([]byte("partial")); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected cancellation, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("canceled stream remained blocked")
	}
}

func TestStreamingUploadRetriesBufferedPart(t *testing.T) {
	var mu sync.Mutex
	attempts := 0
	var firstAttempt []byte
	var completed atomic.Bool
	s := testS3(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Query().Has("uploads"):
			writeXML(w, `<InitiateMultipartUploadResult><UploadId>retry</UploadId></InitiateMultipartUploadResult>`)
		case r.Method == http.MethodPut:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
			}
			if r.URL.Query().Get("partNumber") == "1" {
				mu.Lock()
				defer mu.Unlock()
				attempts++
				if attempts == 1 {
					firstAttempt = body
					writeS3Error(w, http.StatusServiceUnavailable, "ServiceUnavailable")
					return
				}
				if !bytes.Equal(body, firstAttempt) {
					t.Error("retry changed buffered stream data")
				}
			}
			w.Header().Set("ETag", `"part"`)
		case r.Method == http.MethodPost:
			completed.Store(true)
			writeXML(w, `<CompleteMultipartUploadResult><ETag>"complete"</ETag></CompleteMultipartUploadResult>`)
		default:
			t.Errorf("unexpected retry request: %s %s", r.Method, r.URL)
		}
	})
	s.client = s3.New(s.client.Options(), func(o *s3.Options) {
		o.RetryMaxAttempts = 2
		o.Retryer = retry.NewStandard(func(o *retry.StandardOptions) {
			o.MaxAttempts = 2
			o.Backoff = retry.BackoffDelayerFunc(func(int, error) (time.Duration, error) { return 0, nil })
		})
	})
	// NopCloser hides Seek: retries must use the SDK's bounded part buffer.
	body := io.NopCloser(bytes.NewReader(bytes.Repeat([]byte("data"), 3*1024*1024)))
	if err := s.Upload(t.Context(), "key", body); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if attempts != 2 || !completed.Load() {
		t.Fatalf("first part attempts=%d completed=%v", attempts, completed.Load())
	}
}

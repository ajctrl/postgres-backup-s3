package storage

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
)

func isolatedAWS(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY", "AWS_SECRET_KEY", "AWS_SESSION_TOKEN",
		"S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY", "S3_SESSION_TOKEN", "AWS_PROFILE", "AWS_DEFAULT_PROFILE",
		"AWS_ROLE_ARN", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
		"AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", "AWS_REQUEST_CHECKSUM_CALCULATION", "AWS_RESPONSE_CHECKSUM_VALIDATION",
	} {
		t.Setenv(name, "")
	}
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(t.TempDir(), "no-config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "no-credentials"))
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_MAX_ATTEMPTS", "1")
}

func testS3(t *testing.T, handler http.HandlerFunc) *S3 {
	t.Helper()
	isolatedAWS(t)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	s, err := New(context.Background(), Config{
		Bucket: "test-bucket", Region: "us-west-1", Endpoint: server.URL,
		AccessKeyID: "test-key", SecretAccessKey: "test-secret", SessionToken: "test-token",
	})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func writeXML(w http.ResponseWriter, body string) {
	w.Header().Set("Content-Type", "application/xml")
	fmt.Fprint(w, body)
}

func writeS3Error(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	fmt.Fprintf(w, "<Error><Code>%s</Code><Message>test failure</Message></Error>", code)
}

func TestS3SignedRequestsAndFileRoundTrip(t *testing.T) {
	ctx := context.Background()
	key := "/backup//odd%2Fdatabase_2026-09-12T11:20:30.dump"
	versionID := "prior+version/with=punctuation"
	payload := []byte("PostgreSQL custom dump\x00\x01\xff")
	var uploaded atomic.Value
	var deleted atomic.Bool
	s := testS3(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 Credential=test-key/") ||
			!strings.Contains(r.Header.Get("Authorization"), "/us-west-1/s3/aws4_request") {
			t.Errorf("request lacks expected SigV4 credentials: %s", r.Header.Get("Authorization"))
		}
		if r.Header.Get("X-Amz-Security-Token") != "test-token" {
			t.Error("session token was not preserved")
		}
		if r.URL.Query().Has("versioning") {
			if r.URL.Path != "/test-bucket" {
				t.Errorf("bucket path: %q", r.URL.Path)
			}
			writeXML(w, `<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`)
			return
		}
		if r.URL.Path != "/test-bucket/"+key {
			t.Errorf("object key changed: %q", r.URL.Path)
		}
		switch r.Method {
		case http.MethodPut:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
			}
			uploaded.Store(body)
			w.Header().Set("ETag", `"test-etag"`)
		case http.MethodGet:
			if r.URL.Query().Get("versionId") != versionID {
				t.Errorf("version ID changed: %q", r.URL.RawQuery)
			}
			w.Header().Set("ETag", `"test-etag"`)
			w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(payload)-1, len(payload)))
			w.WriteHeader(http.StatusPartialContent)
			w.Write(payload)
		case http.MethodDelete:
			if r.URL.Query().Has("versionId") {
				t.Error("retention must not delete a historical version")
			}
			deleted.Store(true)
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusBadRequest)
		}
	})
	enabled, err := s.VersioningEnabled(ctx)
	if err != nil || !enabled {
		t.Fatalf("versioning = %v, %v", enabled, err)
	}
	path := filepath.Join(t.TempDir(), "backup.dump")
	if err := os.WriteFile(path, payload, 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.Upload(ctx, key, path); err != nil {
		t.Fatal(err)
	}
	if uploaded.Load() == nil || !bytes.Equal(uploaded.Load().([]byte), payload) {
		t.Fatalf("uploaded body differs: %q", uploaded.Load())
	}
	destination := filepath.Join(t.TempDir(), "restored.dump")
	if err := s.Download(ctx, key, versionID, destination); err != nil {
		t.Fatal(err)
	}
	downloaded, err := os.ReadFile(destination)
	if err != nil || !bytes.Equal(downloaded, payload) {
		t.Fatalf("download = %q, %v", downloaded, err)
	}
	info, err := os.Stat(destination)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("download permissions = %v, %v", info, err)
	}
	if err := s.Delete(ctx, key); err != nil || !deleted.Load() {
		t.Fatalf("delete = %v, deleted = %v", err, deleted.Load())
	}
}

func TestS3ListAllPages(t *testing.T) {
	var calls atomic.Int32
	s := testS3(t, func(w http.ResponseWriter, r *http.Request) {
		call := calls.Add(1)
		q := r.URL.Query()
		if q.Get("prefix") != "/backup//odd database_" || q.Get("list-type") != "2" {
			t.Errorf("list query = %s", r.URL.RawQuery)
		}
		var body strings.Builder
		body.WriteString("<ListBucketResult>")
		if call == 1 {
			if q.Get("continuation-token") != "" {
				t.Error("unexpected first-page token")
			}
			body.WriteString("<IsTruncated>true</IsTruncated><NextContinuationToken>next+page/=</NextContinuationToken>")
			for i := 0; i < 1000; i++ {
				fmt.Fprintf(&body, "<Contents><Key>backup-%04d</Key><LastModified>2026-09-12T11:20:30.123Z</LastModified></Contents>", i)
			}
		} else {
			if call != 2 || q.Get("continuation-token") != "next+page/=" {
				t.Errorf("continuation query = %s, calls = %d", r.URL.RawQuery, call)
			}
			body.WriteString("<IsTruncated>false</IsTruncated><Contents><Key>backup-1000</Key><LastModified>2026-09-12T11:20:31Z</LastModified></Contents>")
		}
		body.WriteString("</ListBucketResult>")
		writeXML(w, body.String())
	})
	objects, err := s.List(context.Background(), "/backup//odd database_")
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1001 || objects[1000].Key != "backup-1000" || calls.Load() != 2 {
		t.Fatalf("list count = %d, calls = %d", len(objects), calls.Load())
	}
	want, _ := time.Parse(time.RFC3339Nano, "2026-09-12T11:20:30.123Z")
	if !objects[0].LastModified.Equal(want) {
		t.Errorf("modification time = %v", objects[0].LastModified)
	}
}

func TestS3RejectsIncompleteListings(t *testing.T) {
	for _, test := range []struct{ name, response string }{
		{"missing date", "<Contents><Key>old.dump</Key></Contents>"},
		{"missing key", "<Contents><LastModified>2026-09-12T11:20:30Z</LastModified></Contents>"},
		{"missing token", "<IsTruncated>true</IsTruncated>"},
		{"repeated token", "<IsTruncated>true</IsTruncated><NextContinuationToken>again</NextContinuationToken>"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32
			s := testS3(t, func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) > 2 {
					writeS3Error(w, http.StatusBadRequest, "BadRequest")
					return
				}
				writeXML(w, "<ListBucketResult>"+test.response+"</ListBucketResult>")
			})
			objects, err := s.List(context.Background(), "backup/")
			if err == nil || objects != nil || calls.Load() > 2 {
				t.Fatalf("unsafe listing: objects = %v, err = %v, calls = %d", objects, err, calls.Load())
			}
		})
	}
}

func TestS3MultipartUpload(t *testing.T) {
	const partSize = 5 * 1024 * 1024
	payload := bytes.Repeat([]byte("dump-part-"), (2*partSize)/10+100)
	var mu sync.Mutex
	parts := make(map[int][]byte)
	var completed atomic.Bool
	s := testS3(t, func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		switch {
		case r.Method == http.MethodPost && q.Has("uploads"):
			writeXML(w, "<InitiateMultipartUploadResult><UploadId>upload-id</UploadId></InitiateMultipartUploadResult>")
		case r.Method == http.MethodPut && q.Get("uploadId") == "upload-id":
			n, _ := strconv.Atoi(q.Get("partNumber"))
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
			}
			mu.Lock()
			parts[n] = body
			mu.Unlock()
			w.Header().Set("ETag", fmt.Sprintf(`"part-%d"`, n))
		case r.Method == http.MethodPost && q.Get("uploadId") == "upload-id":
			var body struct {
				Parts []struct {
					Number int    `xml:"PartNumber"`
					ETag   string `xml:"ETag"`
				} `xml:"Part"`
			}
			if err := xml.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if len(body.Parts) != 3 {
				t.Errorf("completion parts = %d", len(body.Parts))
			}
			for i, part := range body.Parts {
				if part.Number != i+1 || part.ETag != fmt.Sprintf(`"part-%d"`, i+1) {
					t.Errorf("completion part = %+v", part)
				}
			}
			completed.Store(true)
			writeXML(w, `<CompleteMultipartUploadResult><ETag>"completed"</ETag></CompleteMultipartUploadResult>`)
		default:
			t.Errorf("unexpected multipart request: %s %s", r.Method, r.URL)
			writeS3Error(w, http.StatusBadRequest, "BadRequest")
		}
	})
	s.transfer = transfermanager.New(s.client, func(o *transfermanager.Options) {
		o.PartSizeBytes = partSize
		o.MultipartUploadThreshold = partSize
		o.Concurrency = 2
	})
	path := filepath.Join(t.TempDir(), "large.dump")
	if err := os.WriteFile(path, payload, 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.Upload(context.Background(), "large.dump", path); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(parts) != 3 || !completed.Load() {
		t.Fatalf("parts = %d, completed = %v", len(parts), completed.Load())
	}
	joined := append(append(parts[1], parts[2]...), parts[3]...)
	if !bytes.Equal(joined, payload) {
		t.Error("multipart upload changed the dump contents")
	}
}

func TestS3CanceledMultipartUploadIsAborted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var aborted atomic.Bool
	s := testS3(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Query().Has("uploads"):
			writeXML(w, "<InitiateMultipartUploadResult><UploadId>canceled-upload</UploadId></InitiateMultipartUploadResult>")
		case r.Method == http.MethodPut:
			cancel()
			writeS3Error(w, http.StatusForbidden, "AccessDenied")
		case r.Method == http.MethodDelete && r.URL.Query().Get("uploadId") == "canceled-upload":
			aborted.Store(true)
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
			writeS3Error(w, http.StatusBadRequest, "BadRequest")
		}
	})
	path := filepath.Join(t.TempDir(), "large.dump")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(20 * 1024 * 1024); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := s.Upload(ctx, "large.dump", path); err == nil {
		t.Fatal("canceled upload succeeded")
	}
	if !aborted.Load() {
		t.Error("multipart upload was not aborted after cancellation")
	}
}

func TestS3FailedDownloadKeepsDestination(t *testing.T) {
	s := testS3(t, func(w http.ResponseWriter, r *http.Request) {
		writeS3Error(w, http.StatusNotFound, "NoSuchKey")
	})
	dir := t.TempDir()
	path := filepath.Join(dir, "existing.dump")
	if err := os.WriteFile(path, []byte("existing"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.Download(context.Background(), "missing.dump", "", path); err == nil {
		t.Fatal("missing object download succeeded")
	}
	body, _ := os.ReadFile(path)
	entries, _ := os.ReadDir(dir)
	if string(body) != "existing" || len(entries) != 1 {
		t.Fatalf("failed download changed destination or left temporary files: body = %q, entries = %v", body, entries)
	}
}

func TestS3MultipartDownloadPinsObject(t *testing.T) {
	payload := bytes.Repeat([]byte("restored-part-"), 1400000)
	for _, versionID := range []string{"", "old+version/=id"} {
		t.Run("version="+versionID, func(t *testing.T) {
			var requests atomic.Int32
			s := testS3(t, func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				var start, end int
				if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil || start >= len(payload) {
					t.Errorf("invalid download range: %q", r.Header.Get("Range"))
					writeS3Error(w, http.StatusBadRequest, "BadRequest")
					return
				}
				if versionID != "" {
					if r.URL.Query().Get("versionId") != versionID {
						t.Error("historical version was not requested on every part")
					}
				} else if start > 0 && r.Header.Get("If-Match") != `"same-object"` {
					t.Error("latest object was not pinned to its original ETag")
				}
				end = min(end, len(payload)-1)
				w.Header().Set("ETag", `"same-object"`)
				w.Header().Set("Content-Length", strconv.Itoa(end-start+1))
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(payload)))
				w.WriteHeader(http.StatusPartialContent)
				w.Write(payload[start : end+1])
			})
			path := filepath.Join(t.TempDir(), "large.dump")
			if err := s.Download(context.Background(), "latest.dump", versionID, path); err != nil {
				t.Fatal(err)
			}
			body, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(body, payload) || requests.Load() != 3 {
				t.Fatalf("multipart download: err = %v, requests = %d, bytes = %d", err, requests.Load(), len(body))
			}
		})
	}
}

func TestS3CredentialOverridesAndDefaultChain(t *testing.T) {
	for _, test := range []struct {
		name       string
		config     Config
		env        map[string]string
		profile    bool
		wantKey    string
		wantSecret string
		wantToken  string
	}{
		{"AWS environment", Config{}, map[string]string{"AWS_ACCESS_KEY_ID": "aws-key", "AWS_SECRET_ACCESS_KEY": "aws-secret", "AWS_SESSION_TOKEN": "aws-token"}, false, "aws-key", "aws-secret", "aws-token"},
		{"S3 key only", Config{}, map[string]string{"S3_ACCESS_KEY_ID": "s3-key", "AWS_SECRET_ACCESS_KEY": "aws-secret", "AWS_SESSION_TOKEN": "aws-token"}, false, "s3-key", "aws-secret", "aws-token"},
		{"S3 secret only", Config{}, map[string]string{"AWS_ACCESS_KEY_ID": "aws-key", "S3_SECRET_ACCESS_KEY": "s3-secret"}, false, "aws-key", "s3-secret", ""},
		{"explicit key only", Config{AccessKeyID: "explicit-key"}, map[string]string{"AWS_SECRET_ACCESS_KEY": "aws-secret", "AWS_SESSION_TOKEN": "aws-token"}, false, "explicit-key", "aws-secret", "aws-token"},
		{"S3 token", Config{}, map[string]string{"AWS_ACCESS_KEY_ID": "aws-key", "AWS_SECRET_ACCESS_KEY": "aws-secret", "S3_SESSION_TOKEN": "s3-token"}, false, "aws-key", "aws-secret", "s3-token"},
		{"shared profile", Config{}, nil, true, "profile-key", "profile-secret", "profile-token"},
	} {
		t.Run(test.name, func(t *testing.T) {
			isolatedAWS(t)
			for key, value := range test.env {
				t.Setenv(key, value)
			}
			if test.profile {
				path := filepath.Join(t.TempDir(), "credentials")
				if err := os.WriteFile(path, []byte("[backup]\naws_access_key_id = profile-key\naws_secret_access_key = profile-secret\naws_session_token = profile-token\n"), 0600); err != nil {
					t.Fatal(err)
				}
				t.Setenv("AWS_SHARED_CREDENTIALS_FILE", path)
				t.Setenv("AWS_PROFILE", "backup")
			}
			cfg := test.config
			cfg.Bucket = "test-bucket"
			s, err := New(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			credentials, err := s.client.Options().Credentials.Retrieve(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if credentials.AccessKeyID != test.wantKey || credentials.SecretAccessKey != test.wantSecret || credentials.SessionToken != test.wantToken {
				t.Error("credentials did not follow the expected override precedence")
			}
		})
	}
}

func TestS3DisabledVersioningAndCancellation(t *testing.T) {
	s := testS3(t, func(w http.ResponseWriter, r *http.Request) {
		writeXML(w, `<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>`)
	})
	enabled, err := s.VersioningEnabled(context.Background())
	if err != nil || enabled {
		t.Fatalf("suspended versioning = %v, %v", enabled, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for name, operation := range map[string]func() error{
		"versioning": func() error { _, err := s.VersioningEnabled(ctx); return err },
		"upload":     func() error { return s.Upload(ctx, "key", "missing") },
		"download":   func() error { return s.Download(ctx, "key", "", "missing") },
		"list":       func() error { _, err := s.List(ctx, ""); return err },
		"delete":     func() error { return s.Delete(ctx, "key") },
	} {
		if err := operation(); !errors.Is(err, context.Canceled) {
			t.Errorf("%s cancellation = %v", name, err)
		}
	}
}

func TestS3ConfigurationValidation(t *testing.T) {
	isolatedAWS(t)
	for _, cfg := range []Config{
		{},
		{Bucket: "test", Endpoint: "localhost:9000"},
		{Bucket: "test", Endpoint: "https://user:password@example.com"},
		{Bucket: "test", Endpoint: "https://example.com?token=secret"},
		{Bucket: "test", AccessKeyID: "incomplete"},
	} {
		if _, err := New(context.Background(), cfg); err == nil {
			t.Errorf("invalid configuration accepted: bucket=%q endpoint=%q", cfg.Bucket, cfg.Endpoint)
		}
	}
}

func TestS3RespectsConfiguredChecksums(t *testing.T) {
	isolatedAWS(t)
	t.Setenv("AWS_REQUEST_CHECKSUM_CALCULATION", "WHEN_REQUIRED")
	s, err := New(context.Background(), Config{Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if s.client.Options().RequestChecksumCalculation != aws.RequestChecksumCalculationWhenRequired {
		t.Error("SDK checksum configuration was not preserved")
	}
}

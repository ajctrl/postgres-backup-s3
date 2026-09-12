// Package tests exercises the built CLI against a local S3 HTTP endpoint and
// fake PostgreSQL/GPG executables. No Python, AWS CLI, or external service is needed.
package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

var cliBinary string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "postgres-backup-s3-cli-tests-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	cliBinary = filepath.Join(dir, "postgres-backup-s3")
	build := exec.Command("go", "build", "-o", cliBinary, "./cmd/postgres-backup-s3")
	build.Dir = ".."
	if output, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build CLI: %v\n%s", err, output)
		os.RemoveAll(dir)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

const fakeTool = `#!/bin/sh
set -eu
command=${0##*/}
line=$command
for arg do line="$line	$arg"; done
printf '%s\n' "$line" >> "$FAKE_LOG"
database=''
output=''
previous=''
decrypt=false
source=''
for arg do
  case "$previous" in -d) database=$arg;; --output|-o) output=$arg;; esac
  [ "$arg" != --decrypt ] || decrypt=true
  previous=$arg
  source=$arg
done
if [ "$command" = "${FAKE_PAUSE_COMMAND:-}" ]; then
  [ -z "${FAKE_CHILD_PID:-}" ] || printf '%s\n' "$$" > "$FAKE_CHILD_PID"
  touch "$FAKE_PAUSE_ENTERED"
  while [ ! -f "$FAKE_PAUSE_RELEASE" ]; do sleep 0.02; done
fi
case "$command" in
  psql)
    [ "${FAKE_DISCOVERY_FAILURE:-}" != true ] || exit 1
    cat "$FAKE_DATABASES_FILE"
    ;;
  pg_dump)
    if [ "$database" = "${FAKE_DUMP_FAILURE:-}" ]; then
      printf 'partial dump\n'
      exit 1
    fi
    printf 'PGDMP %s\n' "$database"
    ;;
  pg_restore)
    case "$(head -c 5 "$source")" in PGDMP) ;; *) exit 3;; esac
    [ "${FAKE_RESTORE_FAILURE:-}" != true ] || exit 1
    ;;
  gpg)
    if [ "$decrypt" = true ]; then
      if [ -n "${FAKE_GPG_FAILURE:-}" ]; then
        case "$(cat "$source")" in *"$FAKE_GPG_FAILURE"*) exit 1;; esac
      fi
      if [ -n "$output" ]; then cp "$source" "$output"; else cat "$source"; fi
    else
      payload=$(cat)
      if [ -n "${FAKE_GPG_FAILURE:-}" ]; then
        case "$payload" in *"$FAKE_GPG_FAILURE"*) exit 1;; esac
      fi
      [ "$output" = - ] || exit 5
      printf '%s\n' "$payload"
    fi
    ;;
  *) exit 4;;
esac
`

type object struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	Size         int64  `xml:"Size"`
}

type request struct {
	Method  string
	Key     string
	Query   url.Values
	Payload string
}

type fakeS3 struct {
	mu            sync.Mutex
	ready         chan struct{}
	readyOnce     sync.Once
	requests      []request
	objects       []object
	versioning    string
	fail          string
	uploadFailure string
	pauseMethod   string
	pauseEntered  chan struct{}
	pauseRelease  chan struct{}
	pauseOnce     sync.Once
	pageSize      int
}

func (s *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Fixture settings are frozen before its first CLI starts. Synchronize
	// with that point explicitly: process startup is not a Go memory barrier.
	<-s.ready
	query := r.URL.Query()
	key := strings.TrimPrefix(r.URL.Path, "/test-bucket/")
	if r.URL.Path == "/test-bucket" || r.URL.Path == "/test-bucket/" {
		key = ""
	}
	body, _ := io.ReadAll(r.Body)
	s.mu.Lock()
	s.requests = append(s.requests, request{r.Method, key, query, string(body)})
	s.mu.Unlock()
	if r.Method == s.pauseMethod {
		s.pauseOnce.Do(func() { close(s.pauseEntered) })
		select {
		case <-s.pauseRelease:
		case <-r.Context().Done():
			return
		}
	}
	w.Header().Set("Content-Type", "application/xml")
	action := r.Method
	if query.Has("versioning") {
		action = "versioning"
	} else if query.Get("list-type") == "2" {
		action = "list"
	}
	if action == s.fail || (action == "PUT" && s.uploadFailure != "" && strings.Contains(key, s.uploadFailure)) {
		w.WriteHeader(http.StatusForbidden)
		io.WriteString(w, `<Error><Code>AccessDenied</Code><Message>Test failure</Message></Error>`)
		return
	}
	switch action {
	case "POST":
		if query.Has("uploads") {
			io.WriteString(w, `<InitiateMultipartUploadResult><UploadId>stream</UploadId></InitiateMultipartUploadResult>`)
		} else if query.Has("uploadId") {
			io.WriteString(w, `<CompleteMultipartUploadResult><ETag>"complete"</ETag></CompleteMultipartUploadResult>`)
		} else {
			w.WriteHeader(http.StatusBadRequest)
		}
	case "versioning":
		xml.NewEncoder(w).Encode(struct {
			XMLName xml.Name `xml:"VersioningConfiguration"`
			Status  string   `xml:"Status,omitempty"`
		}{Status: s.versioning})
	case "list":
		var matching []object
		for _, obj := range s.objects {
			if strings.HasPrefix(obj.Key, query.Get("prefix")) {
				matching = append(matching, obj)
			}
		}
		start, _ := strconv.Atoi(query.Get("continuation-token"))
		end := len(matching)
		if s.pageSize > 0 && end > start+s.pageSize {
			end = start + s.pageSize
		}
		next := ""
		if end < len(matching) {
			next = strconv.Itoa(end)
		}
		xml.NewEncoder(w).Encode(struct {
			XMLName               xml.Name `xml:"ListBucketResult"`
			Name                  string   `xml:"Name"`
			IsTruncated           bool     `xml:"IsTruncated"`
			NextContinuationToken string   `xml:"NextContinuationToken,omitempty"`
			Contents              []object `xml:"Contents"`
		}{Name: "test-bucket", IsTruncated: next != "", NextContinuationToken: next, Contents: matching[start:end]})
	case "PUT":
		w.Header().Set("ETag", `"test-etag"`)
		w.WriteHeader(http.StatusOK)
	case "GET":
		w.Header().Set("Content-Type", "application/octet-stream")
		io.WriteString(w, "PGDMP restored\n")
	case "DELETE":
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Unexpected S3 request: %s %s", r.Method, r.URL)
	}
}

func (s *fakeS3) calls(action string) []request {
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []request
	for _, call := range s.requests {
		kind := call.Method
		if call.Query.Has("versioning") {
			kind = "versioning"
		} else if call.Query.Get("list-type") == "2" {
			kind = "list"
		}
		if action == "" || action == kind {
			result = append(result, call)
		}
	}
	return result
}

type fixture struct {
	t       *testing.T
	dir     string
	runtime string
	log     string
	env     map[string]string
	s3      *fakeS3
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	dir := t.TempDir()
	bin, runtime := filepath.Join(dir, "bin"), filepath.Join(dir, "runtime")
	for _, path := range []string{bin, runtime} {
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	for _, command := range []string{"psql", "pg_dump", "pg_restore", "gpg"} {
		if err := os.WriteFile(filepath.Join(bin, command), []byte(fakeTool), 0700); err != nil {
			t.Fatal(err)
		}
	}
	s3 := &fakeS3{versioning: "Enabled", ready: make(chan struct{})}
	server := httptest.NewServer(s3)
	t.Cleanup(server.Close)
	f := &fixture{t: t, dir: dir, runtime: runtime, log: filepath.Join(dir, "calls.log"), s3: s3}
	f.env = map[string]string{
		"PATH":   bin + string(os.PathListSeparator) + os.Getenv("PATH"),
		"TMPDIR": runtime, "FAKE_LOG": f.log, "FAKE_DATABASES_FILE": filepath.Join(dir, "databases.json"),
		"S3_BUCKET": "test-bucket", "S3_PREFIX": "backup", "S3_REGION": "us-west-1", "S3_ENDPOINT": server.URL,
		"S3_ACCESS_KEY_ID": "test-key", "S3_SECRET_ACCESS_KEY": "test-secret", "AWS_EC2_METADATA_DISABLED": "true",
		"POSTGRES_HOST": "postgres", "POSTGRES_USER": "backup", "POSTGRES_PASSWORD": "test",
	}
	f.databases([]string{"app", "postgres"})
	return f
}

func (f *fixture) databases(value any) {
	f.t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(f.env["FAKE_DATABASES_FILE"], data, 0600); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) command(args ...string) *exec.Cmd {
	f.t.Helper()
	f.s3.readyOnce.Do(func() { close(f.s3.ready) })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	f.t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, cliBinary, args...)
	cmd.Dir = f.dir
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(key, "POSTGRES_") || strings.HasPrefix(key, "PG") || strings.HasPrefix(key, "S3_") || strings.HasPrefix(key, "AWS_") || strings.HasPrefix(key, "BACKUP_") || key == "PASSPHRASE" || key == "SCHEDULE" || key == "TMPDIR" || key == "PATH" {
			continue
		}
		cmd.Env = append(cmd.Env, entry)
	}
	for key, value := range f.env {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	return cmd
}

func (f *fixture) run(wantSuccess bool, args ...string) string {
	f.t.Helper()
	output, err := f.command(args...).CombinedOutput()
	if (err == nil) != wantSuccess {
		f.t.Fatalf("CLI %v: error = %v, want success %t\n%s", args, err, wantSuccess, output)
	}
	f.assertClean()
	return string(output)
}

func (f *fixture) assertClean() {
	f.t.Helper()
	files, err := os.ReadDir(f.runtime)
	if err != nil {
		f.t.Fatal(err)
	}
	if len(files) != 0 {
		f.t.Fatalf("temporary files leaked: %v", files)
	}
}

type processCall struct {
	Command string
	Args    []string
}

func (f *fixture) processes(command string) []processCall {
	f.t.Helper()
	data, err := os.ReadFile(f.log)
	if err != nil && !os.IsNotExist(err) {
		f.t.Fatal(err)
	}
	var calls []processCall
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		fields := strings.Split(line, "\t")
		if command == "" || fields[0] == command {
			calls = append(calls, processCall{fields[0], fields[1:]})
		}
	}
	return calls
}

func (f *fixture) databaseCalls(command string) []string {
	f.t.Helper()
	var databases []string
	for _, call := range f.processes(command) {
		found := false
		for i, arg := range call.Args {
			if arg == "-d" && i+1 < len(call.Args) {
				u, err := url.Parse(call.Args[i+1])
				if err != nil || u.Scheme != "postgresql" {
					f.t.Fatalf("database must be safely represented by a PostgreSQL URI: %v", call.Args)
				}
				databases = append(databases, strings.TrimPrefix(u.Path, "/"))
				found = true
				break
			}
		}
		if !found {
			f.t.Fatalf("missing database argument: %v", call.Args)
		}
	}
	return databases
}

func keys(calls []request) []string {
	result := make([]string, 0, len(calls))
	for _, call := range calls {
		result = append(result, call.Key)
	}
	return result
}

func assertEqual[T any](t *testing.T, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestBackupSingleDatabaseAndLiteralNames(t *testing.T) {
	for _, name := range []string{"app", "with space", "ALL", "with,comma", "a'b", "a/b", "host=other", "$(oops)*"} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			f.env["POSTGRES_DATABASE"] = name
			f.run(true, "backup")
			assertEqual(t, f.databaseCalls("pg_dump"), []string{name})
			calls := f.s3.calls("PUT")
			assertEqual(t, len(calls), 1)
			pattern := "^" + regexp.QuoteMeta("backup/"+name+"_") + `[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.dump$`
			if !regexp.MustCompile(pattern).MatchString(calls[0].Key) {
				t.Fatalf("unexpected timestamp key: %q", calls[0].Key)
			}
			if !strings.Contains(calls[0].Payload, "PGDMP") {
				t.Fatalf("upload does not contain the completed dump: %q", calls[0].Payload)
			}
			assertEqual(t, len(f.s3.calls("list")), 0)
		})
	}
}

func TestDatabaseListOrderDeduplicationAndPrecedence(t *testing.T) {
	f := newFixture(t)
	f.env["POSTGRES_DATABASE"], f.env["POSTGRES_DATABASES"] = "ignored", " billing, ALL,app,billing "
	f.run(true, "backup")
	assertEqual(t, f.databaseCalls("pg_dump"), []string{"billing", "ALL", "app"})
	assertEqual(t, len(f.processes("psql")), 0)
}

func TestDefaultEntrypointAndRunWithoutSchedule(t *testing.T) {
	for _, args := range [][]string{nil, {"run"}} {
		t.Run(fmt.Sprint(args), func(t *testing.T) {
			f := newFixture(t)
			f.env["POSTGRES_DATABASE"] = "app"
			f.env["PGDUMP_EXTRA_OPTS"] = "--no-owner  --exclude-table=*.tmp"
			f.run(true, args...)
			assertEqual(t, f.databaseCalls("pg_dump"), []string{"app"})
			calls := f.processes("pg_dump")
			assertEqual(t, calls[0].Args[len(calls[0].Args)-2:], []string{"--no-owner", "--exclude-table=*.tmp"})
		})
	}
}

func TestUnsetRetentionDoesNotListOrDeleteExistingObjects(t *testing.T) {
	for _, mode := range []string{"timestamp", "fixed"} {
		for _, explicitlyEmpty := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/empty=%t", mode, explicitlyEmpty), func(t *testing.T) {
				f := newFixture(t)
				f.env["POSTGRES_DATABASES"], f.env["BACKUP_FILENAME_MODE"] = "app,billing", mode
				if explicitlyEmpty {
					f.env["BACKUP_KEEP_DAYS"] = ""
				}
				f.s3.fail = "list"
				f.s3.objects = []object{{Key: "backup/app_2000-01-01T00:00:00.dump", LastModified: "2000-01-01T00:00:00Z"}}
				f.run(true, "backup")
				assertEqual(t, len(f.s3.calls("PUT")), 2)
				assertEqual(t, len(f.s3.calls("list")), 0)
				assertEqual(t, len(f.s3.calls("DELETE")), 0)
			})
		}
	}
}

func TestAllDatabaseDiscoveryAndExactExclusions(t *testing.T) {
	f := newFixture(t)
	f.env["POSTGRES_BACKUP_ALL"] = "true"
	f.env["POSTGRES_MAINTENANCE_DB"] = "maintenance db"
	f.env["POSTGRES_DATABASES_EXCLUDE"] = "scratch"
	f.databases([]string{"ALL", "app", "postgres", "scratch", "scratchpad", "app"})
	f.run(true, "backup")
	assertEqual(t, f.databaseCalls("pg_dump"), []string{"ALL", "app", "postgres", "scratchpad"})
	assertEqual(t, f.databaseCalls("psql"), []string{"maintenance db"})
	args := strings.Join(f.processes("psql")[0].Args, " ")
	for _, required := range []string{"NOT datistemplate AND datallowconn", "ORDER BY datname", "ON_ERROR_STOP=1"} {
		if !strings.Contains(args, required) {
			t.Errorf("database discovery missing %q: %s", required, args)
		}
	}
}

func TestInvalidSelectionAndRetentionNeverAccessDatabaseOrS3(t *testing.T) {
	cases := []map[string]string{
		{}, {"POSTGRES_BACKUP_ALL": "ALL"},
		{"POSTGRES_BACKUP_ALL": "true", "POSTGRES_DATABASE": "app"},
		{"POSTGRES_BACKUP_ALL": "true", "POSTGRES_DATABASES": "app"},
		{"POSTGRES_DATABASES": "app,,billing"}, {"POSTGRES_DATABASES": "app,"},
		{"POSTGRES_DATABASE": "app\nother"},
		{"POSTGRES_DATABASE": "app", "POSTGRES_DATABASES_EXCLUDE": "other"},
		{"POSTGRES_DATABASE": "app", "BACKUP_FILENAME_MODE": "wrong"},
	}
	for _, value := range []string{"0", "-1", "abc", "08", "1.5", "999999999999"} {
		for _, mode := range []string{"timestamp", "fixed"} {
			cases = append(cases, map[string]string{"POSTGRES_DATABASE": "app", "BACKUP_FILENAME_MODE": mode, "BACKUP_KEEP_DAYS": value})
		}
	}
	for i, settings := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			f := newFixture(t)
			for key, value := range settings {
				f.env[key] = value
			}
			f.run(false, "backup")
			assertEqual(t, len(f.processes("")), 0)
			assertEqual(t, len(f.s3.calls("")), 0)
		})
	}
}

func TestInvalidOrEmptyDiscoveryNeverDumps(t *testing.T) {
	for i, value := range []any{[]string{}, []string{""}, []string{"a\nb"}, map[string]string{}, []int{1}} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			f := newFixture(t)
			f.env["POSTGRES_BACKUP_ALL"] = "true"
			f.databases(value)
			f.run(false, "backup")
			assertEqual(t, len(f.processes("pg_dump")), 0)
		})
	}
	for _, failure := range []string{"discovery", "excluded"} {
		t.Run(failure, func(t *testing.T) {
			f := newFixture(t)
			f.env["POSTGRES_BACKUP_ALL"] = "true"
			if failure == "discovery" {
				f.env["FAKE_DISCOVERY_FAILURE"] = "true"
			} else {
				f.env["POSTGRES_DATABASES_EXCLUDE"] = "app,postgres"
			}
			f.run(false, "backup")
			assertEqual(t, len(f.processes("pg_dump")), 0)
		})
	}
}

func TestFixedKeysEncodingEncryptionAndVersionRestore(t *testing.T) {
	for _, tc := range []struct{ name, directory string }{
		{"app", "app"}, {"app_2000-01-01T00:00:00", "app_2000-01-01T00%3A00%3A00"},
		{"with space", "with%20space"}, {"a/b", "a%2Fb"}, {"a%2Fb", "a%252Fb"}, {".", "%2E"}, {"..", "%2E%2E"},
	} {
		for _, encrypted := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/encrypted=%t", tc.name, encrypted), func(t *testing.T) {
				f := newFixture(t)
				f.env["POSTGRES_DATABASE"], f.env["BACKUP_FILENAME_MODE"] = tc.name, "fixed"
				suffix := ".dump"
				if encrypted {
					f.env["PASSPHRASE"], suffix = "test-passphrase", ".dump.gpg"
				}
				key := "backup/" + tc.directory + "/latest" + suffix
				f.run(true, "backup")
				assertEqual(t, keys(f.s3.calls("PUT")), []string{key})
				assertEqual(t, len(f.s3.calls("versioning")), 1)
				f.run(true, "restore", "--version-id", "opaque+/version==")
				gets := f.s3.calls("GET")
				assertEqual(t, keys(gets), []string{key})
				assertEqual(t, gets[0].Query.Get("versionId"), "opaque+/version==")
				assertEqual(t, f.databaseCalls("pg_restore"), []string{tc.name})
				assertEqual(t, len(f.s3.calls("versioning")), 1)
				wantGPG := 0
				if encrypted {
					wantGPG = 2
				}
				assertEqual(t, len(f.processes("gpg")), wantGPG)
			})
		}
	}
}

func TestFixedBackupRequiresEnabledVersioning(t *testing.T) {
	for _, status := range []string{"", "Suspended", "error"} {
		t.Run(status, func(t *testing.T) {
			f := newFixture(t)
			f.env["POSTGRES_DATABASE"], f.env["BACKUP_FILENAME_MODE"] = "app", "fixed"
			f.s3.versioning = status
			if status == "error" {
				f.s3.fail = "versioning"
			}
			f.run(false, "backup")
			assertEqual(t, len(f.processes("pg_dump")), 0)
			assertEqual(t, len(f.s3.calls("PUT")), 0)
		})
	}
}

func TestFailuresContinueOtherDatabasesAndSkipUnsafeRetention(t *testing.T) {
	for _, mode := range []string{"timestamp", "fixed"} {
		for _, stage := range []string{"dump", "encrypt", "upload"} {
			t.Run(mode+"/"+stage, func(t *testing.T) {
				f := newFixture(t)
				f.env["POSTGRES_DATABASES"], f.env["BACKUP_FILENAME_MODE"] = "app,billing", mode
				f.env["PASSPHRASE"], f.env["BACKUP_KEEP_DAYS"] = "test", "7"
				switch stage {
				case "dump":
					f.env["FAKE_DUMP_FAILURE"] = "postgresql:///app"
				case "encrypt":
					f.env["FAKE_GPG_FAILURE"] = "postgresql:///app"
				case "upload":
					f.s3.uploadFailure = "app"
				}
				output := f.run(false, "backup")
				if !strings.Contains(output, "1 succeeded, 1 failed") {
					t.Fatalf("incorrect failure summary: %s", output)
				}
				assertEqual(t, f.databaseCalls("pg_dump"), []string{"app", "billing"})
				lists := f.s3.calls("list")
				assertEqual(t, len(lists), 1)
				assertEqual(t, lists[0].Query.Get("prefix"), "backup/billing_")
				uploads := f.s3.calls("PUT")
				want := 1
				if stage == "upload" {
					want = 2
				}
				assertEqual(t, len(uploads), want)
				if !strings.Contains(uploads[len(uploads)-1].Key, "billing") {
					t.Fatalf("later database was not uploaded: %v", keys(uploads))
				}
			})
		}
	}
}

func TestRetentionOnlyDeletesExpiredExactTimestampKeys(t *testing.T) {
	old, recent := "2000-01-01T00:00:00Z", time.Now().UTC().Format(time.RFC3339)
	for _, mode := range []string{"timestamp", "fixed"} {
		t.Run(mode, func(t *testing.T) {
			f := newFixture(t)
			f.env["POSTGRES_DATABASE"], f.env["BACKUP_FILENAME_MODE"], f.env["BACKUP_KEEP_DAYS"] = "app", mode, "7"
			expired := []string{"backup/app_2000-01-01T00:00:00.dump", "backup/app_2000-01-01T00:00:01.dump.gpg"}
			for _, key := range expired {
				f.s3.objects = append(f.s3.objects, object{Key: key, LastModified: old})
			}
			f.s3.objects = append(f.s3.objects, object{Key: "backup/app_2000-01-01T00:00:02.dump", LastModified: recent})
			for _, key := range []string{
				"backup/app/latest.dump", "backup/app/latest.dump.gpg", "backup/app_other_2000-01-01T00:00:00.dump",
				"backup/app_2000-01-01T00%3A00%3A00/latest.dump", "backup/other_2000-01-01T00:00:00.dump",
				"backup/app_notes.dump", "backup/app_2000-01-01T00:00:00.dump.extra", "backup/app_2000-01-01T00:00:00.dump\n",
			} {
				f.s3.objects = append(f.s3.objects, object{Key: key, LastModified: old})
			}
			f.run(true, "backup")
			assertEqual(t, keys(f.s3.calls("DELETE")), expired)
		})
	}
}

func TestRetentionFailuresReportErrorAndContinue(t *testing.T) {
	for _, stage := range []string{"list", "DELETE"} {
		t.Run(stage, func(t *testing.T) {
			f := newFixture(t)
			f.env["POSTGRES_DATABASES"], f.env["BACKUP_KEEP_DAYS"] = "app,billing", "7"
			f.s3.fail = stage
			f.s3.objects = []object{{Key: "backup/app_2000-01-01T00:00:00.dump", LastModified: "2000-01-01T00:00:00Z"}}
			output := f.run(false, "backup")
			if !strings.Contains(output, "2 succeeded, 0 failed") {
				t.Fatalf("incorrect backup summary after retention failure: %s", output)
			}
			assertEqual(t, f.databaseCalls("pg_dump"), []string{"app", "billing"})
		})
	}
}

func TestPrefixLayoutsStayCompatible(t *testing.T) {
	for _, tc := range []struct{ prefix, timestamp, fixed string }{
		{"backup/", "backup//", "backup/"}, {"backup//", "backup///", "backup//"}, {"", "/", ""}, {"/", "//", ""},
	} {
		for _, mode := range []string{"timestamp", "fixed"} {
			t.Run(fmt.Sprintf("%q/%s", tc.prefix, mode), func(t *testing.T) {
				f := newFixture(t)
				f.env["POSTGRES_DATABASE"], f.env["S3_PREFIX"], f.env["BACKUP_FILENAME_MODE"] = "app", tc.prefix, mode
				f.env["BACKUP_KEEP_DAYS"] = "7"
				oldKey := tc.timestamp + "app_2000-01-01T00:00:00.dump"
				f.s3.objects = []object{{Key: oldKey, LastModified: "2000-01-01T00:00:00Z"}}
				f.run(true, "backup")
				assertEqual(t, f.s3.calls("list")[0].Query.Get("prefix"), tc.timestamp+"app_")
				assertEqual(t, keys(f.s3.calls("DELETE")), []string{oldKey})
				if mode == "fixed" {
					key := tc.fixed + "app/latest.dump"
					assertEqual(t, keys(f.s3.calls("PUT")), []string{key})
					f.run(true, "restore")
					assertEqual(t, keys(f.s3.calls("GET")), []string{key})
				} else {
					if !strings.HasPrefix(f.s3.calls("PUT")[0].Key, tc.timestamp+"app_") {
						t.Fatalf("timestamp prefix changed: %v", keys(f.s3.calls("PUT")))
					}
					f.run(true, "restore", "2000-01-01T00:00:00")
					assertEqual(t, keys(f.s3.calls("GET")), []string{oldKey})
				}
			})
		}
	}
}

func TestLatestRestoreExactMatchingAndPagination(t *testing.T) {
	f := newFixture(t)
	f.env["POSTGRES_DATABASE"] = "app"
	f.s3.pageSize = 1000
	for i := 0; i < 1001; i++ {
		key := "backup/app_" + time.Date(2020, 1, 1, 0, 0, i, 0, time.UTC).Format("2006-01-02T15:04:05") + ".dump"
		f.s3.objects = append(f.s3.objects, object{Key: key, LastModified: "2020-01-01T00:00:00Z"})
	}
	want := f.s3.objects[1000].Key
	for _, key := range []string{"backup/app_extra_2030-01-01T00:00:00.dump", "backup/app_2025-01-01T00:00:00.dump.gpg", "backup/app_2040-01-01T00%3A00%3A00/latest.dump"} {
		f.s3.objects = append(f.s3.objects, object{Key: key, LastModified: "2000-01-01T00:00:00Z"})
	}
	f.run(true, "restore")
	assertEqual(t, len(f.s3.calls("list")), 2)
	assertEqual(t, keys(f.s3.calls("GET")), []string{want})
	assertEqual(t, f.databaseCalls("pg_restore"), []string{"app"})
	if !strings.Contains(strings.Join(f.processes("pg_restore")[0].Args, " "), "--exit-on-error") {
		t.Fatal("pg_restore must stop on errors")
	}
}

func TestRestoreFailuresNeverReportSuccess(t *testing.T) {
	for _, stage := range []string{"download", "decrypt", "restore", "list", "empty"} {
		t.Run(stage, func(t *testing.T) {
			f := newFixture(t)
			f.env["POSTGRES_DATABASE"] = "app"
			if stage == "list" || stage == "empty" {
				if stage == "list" {
					f.s3.fail = "list"
				}
			} else {
				f.env["BACKUP_FILENAME_MODE"], f.env["PASSPHRASE"] = "fixed", "test"
				switch stage {
				case "download":
					f.s3.fail = "GET"
				case "decrypt":
					f.env["FAKE_GPG_FAILURE"] = "restored"
				case "restore":
					f.env["FAKE_RESTORE_FAILURE"] = "true"
				}
			}
			output := f.run(false, "restore")
			if strings.Contains(output, "Restore complete") {
				t.Fatalf("restore incorrectly reported success: %s", output)
			}
			want := 0
			if stage == "restore" {
				want = 1
			}
			assertEqual(t, len(f.processes("pg_restore")), want)
		})
	}
}

func TestRestoreInvalidArgumentsNeverAccessServices(t *testing.T) {
	for i, tc := range []struct {
		database, mode string
		args           []string
	}{
		{"", "timestamp", nil}, {"app", "timestamp", []string{"--version-id", "id"}},
		{"app", "timestamp", []string{"bad-timestamp"}}, {"app", "timestamp", []string{"2020-01-01T00:00:00\n"}},
		{"app", "fixed", []string{"2020-01-01T00:00:00"}}, {"app", "fixed", []string{"--version-id", ""}},
		{"app", "timestamp", []string{"a", "b", "c"}},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			f := newFixture(t)
			f.env["POSTGRES_DATABASE"], f.env["BACKUP_FILENAME_MODE"] = tc.database, tc.mode
			f.run(false, append([]string{"restore"}, tc.args...)...)
			assertEqual(t, len(f.processes("")), 0)
			assertEqual(t, len(f.s3.calls("")), 0)
		})
	}
}

func TestBackupLockCoversDumpUploadAndRetentionAcrossProcesses(t *testing.T) {
	for _, stage := range []string{"dump", "PUT", "DELETE"} {
		t.Run(stage, func(t *testing.T) {
			first := newFixture(t)
			first.env["POSTGRES_DATABASE"], first.env["BACKUP_KEEP_DAYS"] = "app", "7"
			first.s3.objects = []object{{Key: "backup/app_2000-01-01T00:00:00.dump", LastModified: "2000-01-01T00:00:00Z"}}
			entered, release := filepath.Join(first.dir, "entered"), filepath.Join(first.dir, "release")
			if stage == "dump" {
				first.env["FAKE_PAUSE_COMMAND"], first.env["FAKE_PAUSE_ENTERED"], first.env["FAKE_PAUSE_RELEASE"] = "pg_dump", entered, release
			} else {
				first.s3.pauseMethod, first.s3.pauseEntered, first.s3.pauseRelease = stage, make(chan struct{}), make(chan struct{})
			}
			var output bytes.Buffer
			cmd := first.command("backup")
			cmd.Stdout, cmd.Stderr = &output, &output
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			var releaseOnce sync.Once
			releaseBackup := func() {
				releaseOnce.Do(func() {
					if stage == "dump" {
						os.WriteFile(release, nil, 0600)
					} else {
						close(first.s3.pauseRelease)
					}
				})
			}
			done := make(chan struct{})
			var waitErr error
			go func() {
				waitErr = cmd.Wait()
				close(done)
			}()
			t.Cleanup(func() {
				releaseBackup()
				select {
				case <-done:
				default:
					cmd.Process.Kill()
					<-done
				}
			})
			if stage == "dump" {
				deadline := time.Now().Add(5 * time.Second)
				for {
					if _, err := os.Stat(entered); err == nil {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("first backup did not enter paused dump")
					}
					time.Sleep(10 * time.Millisecond)
				}
			} else {
				select {
				case <-first.s3.pauseEntered:
				case <-time.After(5 * time.Second):
					t.Fatal("first backup did not enter paused S3 request")
				}
			}
			second := newFixture(t)
			second.env["POSTGRES_DATABASE"] = "app"
			secondOutput := second.run(false, "backup")
			if !strings.Contains(secondOutput, "backup lock") {
				t.Fatalf("overlap failed for a reason other than the shared lock: %s", secondOutput)
			}
			assertEqual(t, len(second.processes("")), 0)
			assertEqual(t, len(second.s3.calls("")), 0)
			releaseBackup()
			select {
			case <-done:
				if waitErr != nil {
					t.Fatalf("first backup failed: %v\n%s", waitErr, output.String())
				}
			case <-time.After(5 * time.Second):
				t.Fatal("first backup did not finish after release")
			}
			first.assertClean()
			second.run(true, "backup")
		})
	}
}

type backgroundCLI struct {
	cmd    *exec.Cmd
	done   chan struct{}
	err    error
	output string
}

func (f *fixture) background(args ...string) *backgroundCLI {
	f.t.Helper()
	log, err := os.CreateTemp(f.dir, "background-*.log")
	if err != nil {
		f.t.Fatal(err)
	}
	p := &backgroundCLI{cmd: f.command(args...), done: make(chan struct{}), output: log.Name()}
	// A file avoids tying Wait to stderr inherited by a deliberately orphaned
	// child in the SIGKILL test. Process completion is independently observable.
	p.cmd.Stdout, p.cmd.Stderr = log, log
	if err := p.cmd.Start(); err != nil {
		log.Close()
		f.t.Fatal(err)
	}
	log.Close()
	go func() {
		p.err = p.cmd.Wait()
		close(p.done)
	}()
	f.t.Cleanup(func() {
		select {
		case <-p.done:
			return
		default:
		}
		p.cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
			p.cmd.Process.Kill()
			<-p.done
		}
	})
	return p
}

func (p *backgroundCLI) wait(t *testing.T) error {
	t.Helper()
	select {
	case <-p.done:
		return p.err
	case <-time.After(5 * time.Second):
		output, _ := os.ReadFile(p.output)
		t.Fatalf("CLI did not terminate\n%s", output)
		return nil
	}
}

func waitForFile(t *testing.T, path string, p *backgroundCLI) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(path); err == nil {
			return
		}
		select {
		case <-p.done:
			output, _ := os.ReadFile(p.output)
			t.Fatalf("CLI exited before creating %s: %v\n%s", path, p.err, output)
		default:
		}
		if time.Now().After(deadline) {
			output, _ := os.ReadFile(p.output)
			t.Fatalf("CLI did not reach paused command\n%s", output)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestScheduledBackupSIGTERMKillsChildAndCleansWorkspace(t *testing.T) {
	f := newFixture(t)
	f.env["POSTGRES_DATABASE"], f.env["SCHEDULE"] = "app", "@every 1s"
	entered, release := filepath.Join(f.dir, "entered"), filepath.Join(f.dir, "release")
	pidPath := filepath.Join(f.dir, "child.pid")
	f.env["FAKE_PAUSE_COMMAND"], f.env["FAKE_PAUSE_ENTERED"], f.env["FAKE_PAUSE_RELEASE"], f.env["FAKE_CHILD_PID"] = "pg_dump", entered, release, pidPath
	p := f.background("run")
	t.Cleanup(func() { os.WriteFile(release, nil, 0600) })
	waitForFile(t, entered, p)
	pidData, err := os.ReadFile(pidPath)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(pidData)))
	if err != nil {
		t.Fatal(err)
	}
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := p.wait(t); err == nil {
		t.Fatal("interrupted scheduled backup returned success")
	} else if exit, ok := err.(*exec.ExitError); !ok || exit.ExitCode() != 143 {
		t.Fatalf("SIGTERM exit = %v, want 143", err)
	}
	if err := syscall.Kill(pid, 0); err != syscall.ESRCH {
		t.Fatalf("dump process %d survived scheduler shutdown: %v", pid, err)
	}
	f.assertClean()
	assertEqual(t, len(f.processes("pg_dump")), 1)
	assertEqual(t, len(f.s3.calls("PUT")), 0)
	next := newFixture(t)
	next.env["POSTGRES_DATABASE"] = "app"
	next.run(true, "backup")
}

func TestAbruptParentExitKeepsBackupLockUntilDumpChildExits(t *testing.T) {
	f := newFixture(t)
	f.env["POSTGRES_DATABASE"] = "app"
	entered, release := filepath.Join(f.dir, "entered"), filepath.Join(f.dir, "release")
	pidPath := filepath.Join(f.dir, "child.pid")
	f.env["FAKE_PAUSE_COMMAND"], f.env["FAKE_PAUSE_ENTERED"], f.env["FAKE_PAUSE_RELEASE"], f.env["FAKE_CHILD_PID"] = "pg_dump", entered, release, pidPath
	p := f.background("backup")
	t.Cleanup(func() { os.WriteFile(release, nil, 0600) })
	waitForFile(t, entered, p)
	pidData, err := os.ReadFile(pidPath)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(pidData)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { syscall.Kill(-pid, syscall.SIGKILL) })
	if err := p.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := p.wait(t); err == nil {
		t.Fatal("SIGKILLed backup returned success")
	}
	next := newFixture(t)
	next.env["POSTGRES_DATABASE"] = "app"
	output := next.run(false, "backup")
	if !strings.Contains(output, "backup lock") {
		t.Fatalf("orphaned dump did not retain the lock: %s", output)
	}
	assertEqual(t, len(next.processes("")), 0)
	assertEqual(t, len(next.s3.calls("")), 0)
	if err := os.WriteFile(release, nil, 0600); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		output, err := next.command("backup").CombinedOutput()
		if err == nil {
			break
		}
		if time.Now().After(deadline) || !strings.Contains(string(output), "backup lock") {
			t.Fatalf("backup lock was not released after the dump child exited: %v\n%s", err, output)
		}
		time.Sleep(10 * time.Millisecond)
	}
	next.assertClean()
	assertEqual(t, len(next.processes("pg_dump")), 1)
	assertEqual(t, len(next.s3.calls("PUT")), 1)
	// SIGKILL cannot execute Go defers; the first workspace is reclaimed by
	// fixture cleanup. The child must keep the lock despite that parent exit.
}

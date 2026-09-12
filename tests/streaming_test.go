package tests

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func streamingFixture(t *testing.T, encrypted bool) *fixture {
	t.Helper()
	f := newFixture(t)
	f.env["POSTGRES_DATABASE"], f.env["BACKUP_FILENAME_MODE"], f.env["BACKUP_KEEP_DAYS"] = "app", "fixed", "7"
	f.env["S3_UPLOAD_PART_SIZE_MB"] = "5"
	f.env["STREAM_ENTERED"], f.env["STREAM_RELEASE"] = filepath.Join(f.dir, "stream-entered"), filepath.Join(f.dir, "stream-release")
	f.env["STREAM_GPG_DONE"] = filepath.Join(f.dir, "gpg-done")
	if encrypted {
		f.env["PASSPHRASE"] = "test"
	}
	scripts := map[string]string{
		"pg_dump": `#!/bin/sh
set -eu
dd if=/dev/zero bs=1048576 count=6 2>/dev/null
exec 1>&-
touch "$STREAM_ENTERED"
while [ ! -f "$STREAM_RELEASE" ]; do sleep 0.02; done
exit "${STREAM_EXIT:-0}"
`,
		"gpg": `#!/bin/sh
set -eu
cat
touch "$STREAM_GPG_DONE"
exit "${STREAM_GPG_EXIT:-0}"
`,
	}
	for name, script := range scripts {
		if err := os.WriteFile(filepath.Join(f.dir, "bin", name), []byte(script), 0700); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { os.WriteFile(f.env["STREAM_RELEASE"], nil, 0600) })
	return f
}

func completedUploads(f *fixture) int {
	count := 0
	for _, request := range f.s3.calls("POST") {
		if request.Query.Has("uploadId") {
			count++
		}
	}
	return count
}

func TestStreamingUploadStartsBeforeDumpExitAndWaitsForSuccess(t *testing.T) {
	for _, mode := range []string{"plaintext", "encrypted", "late-dump-failure"} {
		t.Run(mode, func(t *testing.T) {
			f := streamingFixture(t, mode != "plaintext")
			if mode == "late-dump-failure" {
				f.env["STREAM_EXIT"] = "1"
			}
			f.s3.pauseMethod, f.s3.pauseEntered, f.s3.pauseRelease = "PUT", make(chan struct{}), make(chan struct{})
			p := f.background("backup")
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(f.s3.pauseRelease) }) }
			t.Cleanup(release)
			waitForFile(t, f.env["STREAM_ENTERED"], p)
			if mode != "plaintext" {
				waitForFile(t, f.env["STREAM_GPG_DONE"], p)
			}
			select {
			case <-f.s3.pauseEntered:
			case <-time.After(5 * time.Second):
				t.Fatal("upload did not start while pg_dump was still running")
			}
			assertEqual(t, completedUploads(f), 0)
			// This check runs while the pipeline is active, not just after cleanup.
			f.assertClean()
			if err := os.WriteFile(f.env["STREAM_RELEASE"], nil, 0600); err != nil {
				t.Fatal(err)
			}
			release()
			err := p.wait(t)
			if mode == "late-dump-failure" {
				if err == nil {
					t.Fatal("failed pg_dump published a backup")
				}
				assertEqual(t, completedUploads(f), 0)
				assertEqual(t, len(f.s3.calls("DELETE")), 1)
				assertEqual(t, len(f.s3.calls("list")), 0)
			} else {
				if err != nil {
					output, _ := os.ReadFile(p.output)
					t.Fatalf("streaming failed: %v\n%s", err, output)
				}
				assertEqual(t, completedUploads(f), 1)
			}
		})
	}
}

func TestStreamingFailuresStopOtherProcessesAndReleaseLock(t *testing.T) {
	for _, stage := range []string{"S3", "GPG"} {
		t.Run(stage, func(t *testing.T) {
			f := streamingFixture(t, true)
			if stage == "S3" {
				f.s3.uploadFailure = "app"
			} else {
				f.env["STREAM_GPG_EXIT"] = "1"
			}
			// pg_dump never exits by itself: the failure must cancel the pipeline.
			p := f.background("backup")
			if err := p.wait(t); err == nil {
				t.Fatal("failed pipeline reported success")
			}
			output, _ := os.ReadFile(p.output)
			if strings.Contains(string(output), "Backup complete:") {
				t.Fatalf("failed pipeline reported a completed backup: %s", output)
			}
			assertEqual(t, completedUploads(f), 0)
			assertEqual(t, len(f.s3.calls("list")), 0)
			f.assertClean()
			next := newFixture(t)
			next.env["POSTGRES_DATABASE"] = "app"
			next.run(true, "backup")
		})
	}
}

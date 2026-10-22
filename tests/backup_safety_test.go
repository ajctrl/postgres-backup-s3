package tests

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestUnsafeDumpOptionsNeverStartBackup(t *testing.T) {
	for _, option := range []string{
		"--file=/tmp/out", "--fil=/tmp/out", "-vFplain", "--dbname=other", "-hother", "--version", "other",
		"--data-only", "-va", "--section=data", "--no-owner", "-O",
		"--extra-float-digits=-2", "--extra-float-digits 3", "--binary-upgrade",
		"--no-tablespaces", "--no-table-access-method", "--clean", "--create", "--if-exists",
		"--disable-triggers", "--use-set-session-authorization", "-vc", "-vC",
		"--filter=-", "--filter -", "--no-synchronized-snapshots",
		"--table=orders", "--schema-only", "--enable-row-security", "--no-acl",
	} {
		t.Run(option, func(t *testing.T) {
			f := newFixture(t)
			f.env["POSTGRES_DATABASE"], f.env["PGDUMP_EXTRA_OPTS"] = "app", option
			f.env["BACKUP_KEEP_DAYS"] = "7"
			out := f.run(false, "backup")
			if !strings.Contains(out, "PGDUMP_EXTRA_OPTS") {
				t.Fatalf("missing option validation error: %s", out)
			}
			assertEqual(t, len(f.processes("")), 0)
			assertEqual(t, len(f.s3.calls("")), 0)
		})
	}
}

func TestPartialBackupPermissionAndVersionGate(t *testing.T) {
	for _, tc := range []struct {
		options, partial, version string
		success                   bool
	}{
		{"--table=orders", "true", "18.0", true},
		{"--schema-only", "true", "17.11", true},
		{"--filter=-", "true", "18.0", false},
		{"--data-only", "true", "18.0", false},
		{"--no-synchronized-snapshots", "true", "18.0", false},
		{"--no-statistics", "false", "17.11", false},
		{"--no-statistics", "false", "18.0", true},
		{"--no-statistics", "false", "unknown", false},
	} {
		t.Run(tc.options+"/"+tc.version, func(t *testing.T) {
			f := newFixture(t)
			f.env["POSTGRES_DATABASE"], f.env["PGDUMP_EXTRA_OPTS"] = "app", tc.options
			f.env["BACKUP_ALLOW_PARTIAL"], f.env["FAKE_PG_VERSION"] = tc.partial, tc.version
			f.env["BACKUP_KEEP_DAYS"] = "7"
			output := f.run(tc.success, "backup")
			if !tc.success {
				assertEqual(t, len(f.s3.calls("")), 0)
				assertEqual(t, len(f.processes("psql")), 0)
				for _, call := range f.processes("pg_dump") {
					assertEqual(t, call.Args, []string{"--version"})
				}
			} else if tc.partial == "true" && !strings.Contains(output, "partial database backup explicitly enabled") {
				t.Fatal("missing partial-backup warning")
			}
		})
	}
}

func TestInvalidDumpOutputNeverPublishes(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		for _, script := range []string{"#!/bin/sh\nexit 0\n", "#!/bin/sh\nprintf 'SQL data'\n"} {
			f := newFixture(t)
			f.env["POSTGRES_DATABASE"], f.env["BACKUP_KEEP_DAYS"] = "app", "7"
			if encrypted {
				f.env["PASSPHRASE"] = "test secret"
			}
			if err := os.WriteFile(filepath.Join(f.dir, "bin", "pg_dump"), []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			output := f.run(false, "backup")
			if !strings.Contains(output, "custom-format archive") || strings.Contains(output, "Backup complete") {
				t.Fatalf("invalid dump was not rejected: %s", output)
			}
			assertEqual(t, len(f.s3.calls("")), 0)
		}
	}
}

func TestGPGPassphraseNeverAppearsInProcessArguments(t *testing.T) {
	f := newFixture(t)
	secret := "private-test-passphrase"
	f.env["POSTGRES_DATABASE"], f.env["BACKUP_FILENAME_MODE"], f.env["PASSPHRASE"] = "app", "fixed", secret
	f.run(true, "backup")
	f.run(true, "restore")
	for _, call := range f.processes("") {
		for _, arg := range call.Args {
			if arg == "--passphrase" || strings.Contains(arg, secret) {
				t.Fatal("passphrase exposed in a process argument")
			}
		}
		if call.Command == "gpg" && !strings.Contains(strings.Join(call.Args, " "), "--passphrase-fd 4") {
			t.Fatalf("GPG did not use a separate FD after the inherited lock: %v", call.Args)
		}
	}
}

func TestBackupAndRestoreShareLock(t *testing.T) {
	for _, firstCommand := range []string{"backup", "restore"} {
		t.Run(firstCommand, func(t *testing.T) {
			first := newFixture(t)
			first.env["POSTGRES_DATABASE"], first.env["BACKUP_FILENAME_MODE"] = "app", "fixed"
			entered, release := filepath.Join(first.dir, "entered"), filepath.Join(first.dir, "release")
			tool := "pg_dump"
			if firstCommand == "restore" {
				tool = "pg_restore"
			}
			first.env["FAKE_PAUSE_COMMAND"], first.env["FAKE_PAUSE_ENTERED"], first.env["FAKE_PAUSE_RELEASE"] = tool, entered, release
			p := first.background(firstCommand)
			t.Cleanup(func() { os.WriteFile(release, nil, 0600) })
			waitForFile(t, entered, p)
			for _, secondCommand := range []string{"backup", "restore"} {
				second := newFixture(t)
				second.env["POSTGRES_DATABASE"], second.env["BACKUP_FILENAME_MODE"] = "app", "fixed"
				output := second.run(false, secondCommand)
				if !strings.Contains(output, "backup lock") {
					t.Fatalf("operation did not reject overlapping %s: %s", firstCommand, output)
				}
				assertEqual(t, len(second.s3.calls("")), 0)
				assertEqual(t, len(second.processes("")), 0)
			}
			if err := os.WriteFile(release, nil, 0600); err != nil {
				t.Fatal(err)
			}
			if err := p.wait(t); err != nil {
				t.Fatalf("first operation failed: %v", err)
			}
			first.run(true, "restore")
		})
	}
}

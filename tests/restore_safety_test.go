package tests

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"testing"
)

// Minimal custom-archive TOC for fake pg_restore. The real PostgreSQL integration
// tests exercise archives produced and consumed by the actual client binaries.
func restoreArchiveFixture(dataOnly bool) []byte { return namedArchiveFixture("app", dataOnly) }
func namedArchiveFixture(database string, dataOnly bool) []byte {
	var b bytes.Buffer
	b.Write([]byte{'P', 'G', 'D', 'M', 'P', 1, 16, 0, 4, 8, 1, 0})
	integer := func(n int32) {
		if n < 0 {
			b.WriteByte(1)
			n = -n
		} else {
			b.WriteByte(0)
		}
		binary.Write(&b, binary.LittleEndian, n)
	}
	field := func(s string) { integer(int32(len(s))); b.WriteString(s) }
	for range 7 {
		integer(0)
	}
	field(database)
	for range 2 {
		field("")
	}
	integer(2)
	for i := range 2 {
		integer(int32(i + 1))
		integer(0)
		field("0")
		field("0")
		if i == 0 {
			field("restored")
			field("DATABASE")
			integer(2)
			field("CREATE DATABASE restored")
		} else if dataOnly {
			field("probe")
			field("SEQUENCE SET")
			integer(3)
			field("SELECT setval('probe', 1)")
		} else {
			field("probe")
			field("SEQUENCE")
			integer(2)
			field("CREATE SEQUENCE probe")
		}
		field("")
		field("")
		field("public")
		field("")
		field("")
		integer(0)
		field("owner")
		field("false")
		integer(-1)
		b.Write(make([]byte, 9))
	}
	return b.Bytes()
}

func TestRestoreRejectsDataOnlyAndInvalidArchivesBeforeDatabaseAccess(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		for _, body := range [][]byte{restoreArchiveFixture(true), []byte("PGDMP truncated"), []byte("SQL text")} {
			f := newFixture(t)
			f.env["POSTGRES_DATABASE"], f.env["BACKUP_FILENAME_MODE"] = "app", "fixed"
			if encrypted {
				f.env["PASSPHRASE"] = "test secret"
			}
			f.s3.downloadBody = body
			output := f.run(false, "restore")
			if !strings.Contains(output, "before database access") || strings.Contains(output, "Restore complete") {
				t.Fatalf("missing archive rejection: %s", output)
			}
			assertEqual(t, len(f.processes("pg_restore")), 0)
			assertEqual(t, len(f.processes("psql")), 0)
		}
	}
}

func TestWrongArchiveDatabaseStopsBackupAndRestore(t *testing.T) {
	f := newFixture(t)
	f.env["POSTGRES_DATABASE"], f.env["BACKUP_FILENAME_MODE"] = "app", "fixed"
	other := namedArchiveFixture("other", false)
	if err := os.WriteFile(f.env["FAKE_DUMP_FILE"], other, 0600); err != nil {
		t.Fatal(err)
	}
	f.s3.downloadBody = other
	for _, command := range []string{"restore", "backup"} {
		output := f.run(false, command)
		if !strings.Contains(output, "does not match requested name") {
			t.Fatalf("%s: %s", command, output)
		}
		if command == "restore" {
			assertEqual(t, len(f.processes("psql")), 0)
		}
	}
	assertEqual(t, len(f.s3.calls("PUT")), 0)
	assertEqual(t, len(f.processes("pg_restore")), 0)
}

func TestRestoreOptionsAreExplicitAndIndependent(t *testing.T) {
	settings := []struct{ env, flag string }{
		{"RESTORE_NO_OWNER", "--no-owner"},
		{"RESTORE_NO_TABLESPACES", "--no-tablespaces"},
		{"RESTORE_NO_TABLE_ACCESS_METHOD", "--no-table-access-method"},
	}
	// -1 leaves every setting unset; 0..7 exercise explicit false/true combinations.
	for mask := -1; mask < 8; mask++ {
		t.Run(fmt.Sprint(mask), func(t *testing.T) {
			f := newFixture(t)
			f.env["POSTGRES_DATABASE"], f.env["BACKUP_FILENAME_MODE"] = "app", "fixed"
			for i, setting := range settings {
				if mask >= 0 {
					f.env[setting.env] = fmt.Sprint(mask&(1<<i) != 0)
				}
			}
			f.run(true, "restore")
			calls := f.processes("pg_restore")
			assertEqual(t, len(calls), 1)
			counts := make(map[string]int)
			for _, arg := range calls[0].Args {
				counts[arg]++
			}
			for i, setting := range settings {
				want := 0
				if mask >= 0 && mask&(1<<i) != 0 {
					want = 1
				}
				assertEqual(t, counts[setting.flag], want)
			}
			for _, flag := range []string{"--single-transaction", "--exit-on-error", "--clean", "--if-exists"} {
				assertEqual(t, counts[flag], 1)
			}
		})
	}
}

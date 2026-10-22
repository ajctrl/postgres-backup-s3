package backup

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type tocFixture struct {
	name, kind, namespace, definition, drop string
	section                                 int32
	dependencies                            []string
}

func customArchiveFixture(minor byte, entries ...tocFixture) []byte {
	var b bytes.Buffer
	b.Write([]byte{'P', 'G', 'D', 'M', 'P', 1, minor, 0, 4, 8, 1})
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
	if minor >= 15 {
		b.WriteByte(0)
	} else {
		integer(0)
	}
	for range 7 {
		integer(0)
	}
	for range 3 {
		field("")
	}
	integer(int32(len(entries)))
	for i, e := range entries {
		integer(int32(i + 1))
		integer(0)
		field("0")
		field("0")
		field(e.name)
		field(e.kind)
		integer(e.section)
		field(e.definition)
		field(e.drop)
		field("")
		field(e.namespace)
		field("")
		field("")
		if minor >= 16 {
			integer(0)
		}
		field("owner")
		field("false")
		for _, dep := range e.dependencies {
			field(dep)
		}
		integer(-1)
		b.Write(make([]byte, 9))
	}
	return b.Bytes()
}

func TestArchiveSchemaValidation(t *testing.T) {
	metadata := tocFixture{name: "db", kind: "DATABASE", definition: "CREATE DATABASE db", section: 2}
	sequence := tocFixture{name: "name with spaces\nand newline", namespace: "odd schema", kind: "SEQUENCE", definition: "CREATE SEQUENCE", drop: "DROP SEQUENCE", section: 2}
	setval := tocFixture{name: sequence.name, namespace: sequence.namespace, kind: "SEQUENCE SET", definition: "SELECT setval", section: 3}
	table := tocFixture{name: "probe", namespace: "public", kind: "TABLE", definition: "CREATE TABLE", drop: "DROP TABLE", section: 2}
	data := tocFixture{name: table.name, namespace: table.namespace, kind: "TABLE DATA", section: 3}
	extension := tocFixture{name: "ext", kind: "EXTENSION", definition: "CREATE EXTENSION", drop: "DROP EXTENSION", section: 2}
	extensionData := data
	extensionData.dependencies = []string{"1"}
	wrongNamespace := sequence
	wrongNamespace.namespace = "another"
	blob := tocFixture{name: "90000", kind: "BLOB", section: 3, definition: "SELECT lo_create(90000)", drop: "SELECT lo_unlink(90000)"}
	blobMetadata := tocFixture{name: "90000", kind: "BLOB METADATA", section: 3, definition: "90000\n", drop: "-- dummy"}
	blobs := tocFixture{name: "BLOBS", kind: "BLOBS", section: 3}
	for _, tc := range []struct {
		name    string
		entries []tocFixture
		valid   bool
	}{
		{"empty-full", []tocFixture{metadata}, true},
		{"normal", []tocFixture{metadata, sequence, table, data, setval}, true},
		{"schema-only", []tocFixture{metadata, sequence, table}, true},
		{"data-only", []tocFixture{metadata, data, setval}, false},
		{"table-data-only", []tocFixture{metadata, data}, false},
		{"mixed-missing-sequence", []tocFixture{metadata, table, data, setval}, false},
		{"wrong-namespace", []tocFixture{wrongNamespace, setval}, false},
		{"extension-config", []tocFixture{extension, extensionData}, true},
		{"extension-does-not-exempt-sequences", []tocFixture{extension, setval}, false},
		{"missing-extension", []tocFixture{metadata, extensionData}, false},
		{"legacy-lo-only", []tocFixture{metadata, blob, blobs}, false},
		{"grouped-lo-only", []tocFixture{metadata, blobMetadata, blobs}, false},
		{"lo-data-without-metadata", []tocFixture{metadata, blobs}, false},
		{"full-with-lo", []tocFixture{metadata, table, data, blobMetadata, blobs}, true},
		{"statistics-without-object-definitions", []tocFixture{metadata, {kind: "STATISTICS DATA", section: 3}}, false},
		{"no-entries", nil, false},
	} {
		for _, minor := range []byte{14, 15, 16} {
			t.Run(fmt.Sprintf("%s/1.%d", tc.name, minor), func(t *testing.T) {
				err := checkArchiveSchema(t.Context(), bytes.NewReader(customArchiveFixture(minor, tc.entries...)))
				if (err == nil) != tc.valid {
					t.Fatalf("valid=%v: %v", tc.valid, err)
				}
			})
		}
	}
	archive := customArchiveFixture(16, sequence, setval)
	for n := 0; n < len(archive); n++ {
		if err := checkArchiveSchema(t.Context(), bytes.NewReader(archive[:n])); err == nil {
			t.Fatalf("accepted truncated TOC at byte %d", n)
		}
	}
	archive[6] = 17
	if err := checkArchiveSchema(t.Context(), bytes.NewReader(archive)); err == nil {
		t.Fatal("accepted unknown layout")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := checkArchiveSchema(ctx, bytes.NewReader(customArchiveFixture(16, sequence))); err != context.Canceled {
		t.Fatalf("cancellation: %v", err)
	}
}

// Run with PostgreSQL server tools on PATH (or PG_TEST_BINDIR set). No existing
// server is accessed: this creates a disposable cluster with a private socket.
func TestArchiveSchemaWithPostgreSQL(t *testing.T) {
	bin := postgresTestBin(t)
	dir, err := os.MkdirTemp("", "pgs3-archive-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	run := func(tool string, args ...string) []byte {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, filepath.Join(bin, tool), args...)
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s: %v\n%s", tool, err, output)
		}
		return output
	}
	cluster := filepath.Join(dir, "cluster")
	run("initdb", "-D", cluster, "-U", "archive_test", "-A", "trust", "--no-locale")
	opts := "-k '" + strings.ReplaceAll(dir, "'", "'\\''") + "' -p 55439 -c listen_addresses=''"
	run("pg_ctl", "-D", cluster, "-l", filepath.Join(dir, "postgres.log"), "-o", opts, "-w", "start")
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if output, err := exec.CommandContext(ctx, filepath.Join(bin, "pg_ctl"), "-D", cluster, "-m", "immediate", "-w", "stop").CombinedOutput(); err != nil {
			t.Errorf("stop temporary PostgreSQL: %v\n%s", err, output)
		}
	})
	connection := []string{"-h", dir, "-p", "55439", "-U", "archive_test", "-d", "postgres"}
	sql := func(query string) {
		run("psql", append(append([]string{}, connection...), "-v", "ON_ERROR_STOP=1", "-c", query)...)
	}
	dump := func(options ...string) []byte {
		args := append(append([]string{}, connection...), "--format=custom")
		return run("pg_dump", append(args, options...)...)
	}
	if err := checkArchiveSchema(t.Context(), bytes.NewReader(dump())); err != nil {
		t.Fatalf("empty database: %v", err)
	}
	sql("SELECT lo_from_bytea(90000, convert_to('saved', 'UTF8'))")
	// Both forms are ambiguous without any independently verifiable object
	// definitions. Fail closed instead of treating DATABASE/session entries as proof.
	for _, options := range [][]string{nil, {"--data-only"}, {"--section=data"}} {
		if err := checkArchiveSchema(t.Context(), bytes.NewReader(dump(options...))); err == nil {
			t.Fatalf("accepted ambiguous large-object-only archive: %v", options)
		}
	}
	sql(`CREATE TABLE probe(id bigint GENERATED ALWAYS AS IDENTITY, value text);
CREATE SEQUENCE "odd sequence
name"; INSERT INTO probe(value) VALUES ('saved');
CREATE MATERIALIZED VIEW mv AS SELECT * FROM probe;`)
	for _, tc := range []struct {
		options []string
		valid   bool
	}{
		{nil, true}, {[]string{"--schema-only"}, true}, {[]string{"-t", "probe"}, true},
		{[]string{"--section=pre-data", "--section=data"}, true},
		{[]string{"--data-only"}, false}, {[]string{"--section=data"}, false},
	} {
		err := checkArchiveSchema(t.Context(), bytes.NewReader(dump(tc.options...)))
		if (err == nil) != tc.valid {
			t.Errorf("options %v: %v", tc.options, err)
		}
	}
	available := run("psql", append(append([]string{}, connection...), "-Atc", "SELECT count(*) FROM pg_available_extensions WHERE name='hstore'")...)
	if strings.TrimSpace(string(available)) != "1" {
		postgresTestUnavailable(t, "extension configuration check requires hstore")
	}
	// Model extension configuration metadata with a table registered to
	// hstore. pg_dump omits its TABLE entry and leaves a missing dependency.
	sql(`CREATE EXTENSION hstore; CREATE TABLE extension_config(value text); ALTER EXTENSION hstore ADD TABLE extension_config;
UPDATE pg_extension SET extconfig=ARRAY['extension_config'::regclass::oid], extcondition=ARRAY[''] WHERE extname='hstore';`)
	if err := checkArchiveSchema(t.Context(), bytes.NewReader(dump())); err != nil {
		t.Fatalf("extension configuration: %v", err)
	}
}

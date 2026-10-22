package backup

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
)

// Validate the uncompressed custom-archive TOC before connecting to PostgreSQL.
// pg_restore --list is intended for humans: names containing spaces/newlines
// cannot be reliably matched there. Read the length-prefixed metadata instead.
// Layout: PostgreSQL src/bin/pg_dump/{pg_backup_archiver,pg_backup_custom}.c,
// ReadHead, ReadToc, and _ReadExtraToc. Archive versions 1.14 through 1.16 cover
// the supported PostgreSQL clients; unknown layouts fail closed.
func validateRestoreArchive(ctx context.Context, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := checkArchiveSchema(ctx, f); err != nil {
		return fmt.Errorf("restore archive rejected before database access: %w", err)
	}
	return nil
}

type archiveReader struct {
	r       *bufio.Reader
	intSize int
	err     error
}

func (r *archiveReader) bytes(n int) []byte {
	if r.err != nil {
		return nil
	}
	b := make([]byte, n)
	_, r.err = io.ReadFull(r.r, b)
	return b
}

func (r *archiveReader) integer() int64 {
	b := r.bytes(1 + r.intSize)
	if r.err != nil {
		return 0
	}
	var raw [8]byte
	copy(raw[:], b[1:])
	v := binary.LittleEndian.Uint64(raw[:])
	if b[0] > 1 || v > 0x7fffffff {
		r.err = fmt.Errorf("invalid archive integer")
		return 0
	}
	if b[0] == 1 {
		return -int64(v)
	}
	return int64(v)
}

// SQL definitions can be large. Skip them without allocating their contents.
func (r *archiveReader) field(keep bool) (string, int64) {
	n := r.integer()
	if r.err != nil || n == -1 {
		return "", n
	}
	if n < 0 || (keep && n > 1024*1024) {
		r.err = fmt.Errorf("invalid archive metadata length")
		return "", n
	}
	if keep {
		return string(r.bytes(int(n))), n
	}
	_, r.err = io.CopyN(io.Discard, r.r, n)
	return "", n
}

func checkArchiveSchema(ctx context.Context, input io.Reader) error {
	return checkArchive(ctx, input, false)
}

// Retain only the command prefix, never an entire SQL definition.
func (r *archiveReader) definitionPrefix() (string, int64) {
	n := r.integer()
	if r.err != nil || n == -1 {
		return "", n
	}
	if n < 0 {
		r.err = fmt.Errorf("invalid archive definition length")
		return "", n
	}
	prefix := r.bytes(int(min(n, 32)))
	if r.err == nil {
		_, r.err = io.CopyN(io.Discard, r.r, n-int64(len(prefix)))
	}
	return string(prefix), n
}

func checkArchive(ctx context.Context, input io.Reader, rejectUnlogged bool) error {
	r := &archiveReader{r: bufio.NewReader(input)}
	header := r.bytes(11)
	if r.err != nil {
		return r.err
	}
	minor := header[6]
	if string(header[:5]) != "PGDMP" || header[5] != 1 || minor < 14 || minor > 16 || header[10] != 1 {
		return fmt.Errorf("unsupported custom archive format (expected versions 1.14 through 1.16)")
	}
	r.intSize = int(header[8])
	offsetSize := int(header[9])
	if (r.intSize != 4 && r.intSize != 8) || (offsetSize != 4 && offsetSize != 8) {
		return fmt.Errorf("invalid archive integer/offset size")
	}
	if minor >= 15 {
		r.bytes(1) // Compression algorithm; the TOC itself is never compressed.
	} else {
		r.integer() // Compression level in older archives.
	}
	for range 7 {
		r.integer() // Creation time.
	}
	for range 3 {
		r.field(false) // Database and producer/server versions.
	}
	count := r.integer()
	if count < 0 {
		return fmt.Errorf("invalid archive entry count")
	}
	type object struct{ namespace, name string }
	type dataObject struct {
		object
		kind string
	}
	definitions := make(map[object]string)
	var required []dataObject
	hasSchema := false
	hasObjectSchema, hasData := false, false
	hasExtension := false
	for i := int64(0); i < count && r.err == nil; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		id := r.integer()
		if id <= 0 {
			return fmt.Errorf("invalid archive entry ID")
		}
		r.integer()    // Has a data dumper.
		r.field(false) // Catalog table OID.
		r.field(false) // Object OID.
		name, _ := r.field(true)
		kind, _ := r.field(true)
		section := r.integer()
		definitionPrefix, definitionSize := r.definitionPrefix()
		// pg_dump emits these canonical prefixes for UNLOGGED relations. Check
		// the actual dump as well as the preflight query to cover intervening DDL.
		if rejectUnlogged && section == 2 &&
			((kind == "TABLE" && strings.HasPrefix(definitionPrefix, "CREATE UNLOGGED TABLE ")) ||
				(kind == "SEQUENCE" && strings.HasPrefix(definitionPrefix, "CREATE UNLOGGED SEQUENCE "))) {
			return fmt.Errorf("standby archive contains UNLOGGED definitions with omitted contents; BACKUP_ALLOW_PARTIAL=true is required")
		}
		_, dropSize := r.field(false)
		r.field(false) // COPY statement.
		namespace, _ := r.field(true)
		r.field(false) // Tablespace.
		r.field(false) // Table access method.
		if minor >= 16 {
			r.integer() // Relation kind.
		}
		r.field(false) // Owner.
		r.field(false) // Historical WITH OIDS flag.
		for r.err == nil {
			_, n := r.field(false) // Dependency list terminated by a null string.
			if n == -1 {
				break
			}
		}
		r.bytes(1 + offsetSize) // Data offset and its status byte.
		key := object{namespace, name}
		hasData = hasData || section == 3       // SECTION_DATA, including large objects/statistics.
		if section == 2 && definitionSize > 0 { // SECTION_PRE_DATA
			hasSchema = true
			// Session/database metadata also exists in data-only dumps. It is
			// not proof that object definitions were included. In particular,
			// a full LO-only dump and a data-only LO dump can have identical TOCs.
			switch kind {
			case "ENCODING", "STDSTRINGS", "SEARCHPATH", "DATABASE", "DATABASE PROPERTIES":
			default:
				hasObjectSchema = hasObjectSchema || dropSize > 0
			}
			if kind == "TABLE" || kind == "FOREIGN TABLE" || kind == "MATERIALIZED VIEW" || kind == "SEQUENCE" {
				definitions[key] = kind
			}
			if kind == "EXTENSION" && dropSize > 0 {
				hasExtension = true
			}
		}
		var requiredKind string
		switch kind {
		case "SEQUENCE SET":
			requiredKind = "SEQUENCE"
		case "TABLE DATA":
			requiredKind = "TABLE"
		case "MATERIALIZED VIEW DATA":
			requiredKind = "MATERIALIZED VIEW"
		}
		if requiredKind != "" {
			required = append(required, dataObject{key, requiredKind})
		}
	}
	if r.err != nil {
		return fmt.Errorf("invalid archive table of contents: %w", r.err)
	}
	if !hasSchema {
		return fmt.Errorf("data-only or definition-free archives are not supported; include pre-data object definitions")
	}
	if hasData && !hasObjectSchema {
		return fmt.Errorf("data-only or ambiguous archive: data has no independently verifiable object definitions (large-object-only archives are not supported)")
	}
	for _, data := range required {
		got := definitions[data.object]
		if got == data.kind || (data.kind == "TABLE" && got == "FOREIGN TABLE") {
			continue
		}
		// Extension configuration tables have no separate TABLE definition, and
		// their TABLE DATA dependencies can refer to omitted table entries.
		// Their row changes are transactional. SEQUENCE SET is different: require
		// a matching definition even when an extension is present, since setval on
		// an existing sequence does not roll back.
		if data.kind == "TABLE" && hasExtension {
			continue
		}
		return fmt.Errorf("archive data for %q.%q has no matching %s definition; data-only or incomplete archives are not supported", data.namespace, data.name, data.kind)
	}
	return ctx.Err()
}

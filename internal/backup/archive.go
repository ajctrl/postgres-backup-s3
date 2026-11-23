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
func validateRestoreArchive(ctx context.Context, f *os.File, metadata *archiveMetadata) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	metadata.layout = &tocLayout{}
	if err := inspectArchive(ctx, f, archiveChecks{metadata: metadata}); err != nil {
		return fmt.Errorf("restore archive rejected before database access: %w", err)
	}
	return nil
}

type archiveReader struct {
	r       *bufio.Reader
	scratch [512]byte
	intSize int
	err     error
}

// fixed reads small fields into storage reused for the lifetime of the parser.
func (r *archiveReader) fixed(n int) []byte {
	if r.err != nil {
		return nil
	}
	b := r.scratch[:n]
	_, r.err = io.ReadFull(r.r, b)
	return b
}

func (r *archiveReader) skip(n int64) {
	for n > 0 && r.err == nil {
		count := int(min(n, int64(r.r.Size())))
		_, r.err = r.r.Discard(count)
		n -= int64(count)
	}
}

func (r *archiveReader) integer() int64 {
	b := r.fixed(1 + r.intSize)
	if r.err != nil {
		return 0
	}
	v := uint64(binary.LittleEndian.Uint32(b[1:]))
	if r.intSize == 8 {
		v = binary.LittleEndian.Uint64(b[1:])
	}
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
		if n <= int64(len(r.scratch)) {
			return string(r.fixed(int(n))), n
		}
		buf := make([]byte, int(n))
		_, r.err = io.ReadFull(r.r, buf)
		return string(buf), n
	}
	r.skip(n)
	return "", n
}

// Retain only the command prefix, never an entire SQL definition.
func (r *archiveReader) definitionPrefix(limit int64) (string, int64) {
	n := r.integer()
	if r.err != nil || n == -1 {
		return "", n
	}
	if n < 0 {
		r.err = fmt.Errorf("invalid archive definition length")
		return "", n
	}
	prefix := r.fixed(int(min(n, limit)))
	if r.err == nil {
		r.skip(n - int64(len(prefix)))
	}
	return string(prefix), n
}

type extensionTable struct {
	namespace, name, oid string
	kind, owner          string
}

type archiveMetadata struct {
	database        string
	layout          *tocLayout
	proofs          []extensionProof
	emptyMappings   []string
	extensions      []string
	extensionTables []extensionTable
	encoding        string
	subscriptions   []string
	refreshesViews  bool
}

type archiveChecks struct {
	database              string
	rejectUnlogged        bool
	requireMappingOptions bool
	metadata              *archiveMetadata
}

// pg_dump quotes mapping and server identifiers with doubled double quotes.
// The options list begins after both identifiers, before any option values.
func hasMappingOptions(prefix string) bool {
	quoted := false
	for _, c := range prefix {
		switch c {
		case '"':
			quoted = !quoted
		case '(':
			if !quoted {
				return true
			}
		}
	}
	return false
}

func inspectArchive(ctx context.Context, input io.Reader, checks archiveChecks) error {
	var layout *tocLayout
	if checks.metadata != nil {
		layout = checks.metadata.layout
	}
	var counter countedArchive
	if layout != nil {
		counter.Reader = input
		input = &counter
	}
	r := &archiveReader{r: bufio.NewReader(input)}
	position := func() int64 { return counter.n - int64(r.r.Buffered()) }
	header := r.fixed(11)
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
		r.fixed(1) // Compression algorithm; the TOC itself is never compressed.
	} else {
		r.integer() // Compression level in older archives.
	}
	for range 7 {
		r.integer() // Creation time.
	}
	database, _ := r.field(checks.metadata != nil || checks.database != "")
	if checks.database != "" && database != checks.database {
		return fmt.Errorf("archive database %q does not match requested name %q", database, checks.database)
	}
	if checks.metadata != nil {
		checks.metadata.database = database
	}
	r.field(false) // Producer and server versions.
	r.field(false)
	if layout != nil {
		layout.countAt = position()
		layout.intSize = r.intSize
		layout.offsetSize = offsetSize
		layout.minor = minor
	}
	count := r.integer()
	if layout != nil {
		layout.count = count
	}
	if count < 0 {
		return fmt.Errorf("invalid archive entry count")
	}
	type object struct{ namespace, name string }
	type dataObject struct {
		object
		kind, oid string
		span      tocSpan
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
		entryStart := position()
		id := r.integer()
		if layout != nil {
			layout.maxID = max(layout.maxID, id)
		}
		if id <= 0 {
			return fmt.Errorf("invalid archive entry ID")
		}
		r.integer()                               // Has a data dumper.
		r.field(false)                            // Catalog table OID.
		oid, _ := r.field(checks.metadata != nil) // Source relation OID for TABLE DATA.
		name, _ := r.field(true)
		kind, _ := r.field(true)
		section := r.integer()
		limit := int64(32)
		if kind == "ENCODING" && checks.metadata != nil {
			limit = 128
		}
		if kind == "USER MAPPING" && checks.requireMappingOptions {
			// Two PostgreSQL identifiers (63 bytes each) plus quoting and keywords.
			limit = 512
		}
		definitionAt := position()
		definitionPrefix, definitionSize := r.definitionPrefix(limit)
		if kind == "EXTENSION" && layout != nil {
			if layout.extensionSQL == nil {
				layout.extensionSQL = make(map[string]tocSpan)
			}
			layout.extensionSQL[name] = tocSpan{definitionAt, position()}
		}
		if checks.requireMappingOptions && kind == "USER MAPPING" && !hasMappingOptions(definitionPrefix) {
			if checks.metadata == nil {
				return fmt.Errorf("cannot verify USER MAPPING without source metadata")
			}
			checks.metadata.emptyMappings = append(checks.metadata.emptyMappings, name)
		}
		if m := checks.metadata; m != nil {
			switch kind {
			case "SUBSCRIPTION":
				m.subscriptions = append(m.subscriptions, name)
			case "MATERIALIZED VIEW DATA":
				m.refreshesViews = true
			case "ENCODING":
				raw, ok := strings.CutPrefix(strings.TrimSpace(definitionPrefix), "SET client_encoding = '")
				if encoding, complete := strings.CutSuffix(raw, "';"); ok && complete {
					m.encoding = encoding
				}
			}
		}
		// pg_dump emits these canonical prefixes for UNLOGGED relations. Check
		// the actual dump as well as the preflight query to cover intervening DDL.
		if checks.rejectUnlogged && section == 2 &&
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
		offsetAt := position()
		offset := r.fixed(1 + offsetSize)
		if layout != nil && r.err == nil && offset[0] == 2 { // K_OFFSET_POS_SET
			var value [8]byte
			copy(value[:], offset[1:])
			layout.offsets = append(layout.offsets, tocOffset{offsetAt + 1, binary.LittleEndian.Uint64(value[:])})
		}
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
				if checks.metadata != nil {
					checks.metadata.extensions = append(checks.metadata.extensions, name)
				}
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
			required = append(required, dataObject{key, requiredKind, oid, tocSpan{entryStart, position()}})
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
		if data.kind == "TABLE" && got == "FOREIGN TABLE" {
			return fmt.Errorf("WARNING: archive contains foreign table data for %q.%q; skipping database to avoid writing to the external server", data.namespace, data.name)
		}
		if got == data.kind {
			continue
		}
		// pg_dump omits the definition of an extension configuration table.
		// Verify its identity against the catalog and guard its restore session.
		if (data.kind == "TABLE" || (data.kind == "SEQUENCE" && checks.metadata != nil)) && got == "" && hasExtension {
			if checks.metadata != nil {
				if layout != nil {
					layout.spans = append(layout.spans, data.span)
				}
				checks.metadata.extensionTables = append(checks.metadata.extensionTables, extensionTable{namespace: data.namespace, name: data.name, oid: data.oid, kind: data.kind})
			}
			continue
		}
		return fmt.Errorf("archive data for %q.%q has no matching %s definition; data-only or incomplete archives are not supported", data.namespace, data.name, data.kind)
	}
	return ctx.Err()
}

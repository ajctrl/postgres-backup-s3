package backup

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"
)

// The custom format stores an uncompressed TOC followed by data blocks.
// Bracket extension TABLE DATA entries with SQL in pg_restore's own serial
// transaction. This preserves COPY/INSERT, large-object handling, the native
// cleanup order and subsequent refreshes without parsing or expanding row data.
type tocSpan struct{ start, end int64 }
type tocOffset struct {
	at    int64
	value uint64
}
type tocLayout struct {
	minor                 byte
	intSize, offsetSize   int
	countAt, count, maxID int64
	offsets               []tocOffset
	spans                 []tocSpan
	extensionSQL          map[string]tocSpan
}
type countedArchive struct {
	io.Reader
	n int64
}

func (r *countedArchive) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.n += int64(n)
	return n, err
}

func archiveInt(n int64, size int) []byte {
	b := make([]byte, size+1)
	if n < 0 {
		b[0] = 1
		n = -n
	}
	binary.LittleEndian.PutUint32(b[1:], uint32(n))
	return b
}

// A definition-only entry has no owner, drop command, dependencies or data
// offset. Archive integers have a separate sign byte; null strings use -1.
func (l *tocLayout) sqlEntry(id int64, sql string) []byte {
	var buf bytes.Buffer
	integer := func(n int64) { buf.Write(archiveInt(n, l.intSize)) }
	field := func(s string) { integer(int64(len(s))); buf.WriteString(s) }
	integer(id)
	integer(0)
	field("0")
	field("0")
	field("extension configuration check")
	field("BACKUP CHECK")
	integer(2) // SECTION_PRE_DATA makes this a schema command in the serial pass.
	field(sql)
	field("")
	field("")
	integer(-1)
	integer(-1)
	integer(-1) // namespace, tablespace, table access method
	if l.minor >= 16 {
		integer(0)
	}
	field("")
	field("false")
	integer(-1)
	buf.WriteByte(3) // K_OFFSET_NO_DATA
	buf.Write(make([]byte, l.offsetSize))
	return buf.Bytes()
}

// Quoted identifiers are written in the archive's client encoding, just as
// pg_dump writes its own DDL. Preserve identifier bytes, including quotes.
func quotedIdentifier(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// A scalar division by the count aborts the transaction if no row matches.
// This works in a database with no installed procedural language.
func requireCatalogRow(query string) string {
	return "SELECT 1 / pg_catalog.count(*) FROM (" + query + ") AS verified;\n"
}

func beginExtensionRows(table extensionTable) string {
	nameExpr := func(name string) string {
		return fmt.Sprintf("pg_catalog.convert_from(pg_catalog.decode('%x','hex'),pg_catalog.current_setting('client_encoding')::pg_catalog.name)", name)
	}
	relation := quotedIdentifier(table.namespace) + "." + quotedIdentifier(table.name)
	member := fmt.Sprintf(`SELECT FROM pg_catalog.pg_class c
 JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
 JOIN pg_catalog.pg_extension e ON c.oid=ANY(e.extconfig)
 JOIN pg_catalog.pg_depend d ON d.classid='pg_catalog.pg_class'::pg_catalog.regclass AND d.objid=c.oid AND d.objsubid=0
 AND d.refclassid='pg_catalog.pg_extension'::pg_catalog.regclass AND d.refobjid=e.oid AND d.deptype='e'
 WHERE n.nspname::text=%s AND c.relname::text=%s AND e.extname::text=%s`, nameExpr(table.namespace), nameExpr(table.name), nameExpr(table.owner))
	if table.kind == "SEQUENCE" {
		// RESTART locks the sequence and makes the following setval transactional.
		return "ALTER SEQUENCE " + relation + " RESTART;\n" + requireCatalogRow(member+" AND c.relkind='S'")
	}
	localTable := member + ` AND c.relkind IN ('r','p') AND (c.relkind <> 'p' OR NOT EXISTS (
 WITH RECURSIVE partitions(id) AS (SELECT c.oid UNION SELECT inhrelid FROM pg_catalog.pg_inherits JOIN partitions ON inhparent=id)
 SELECT FROM partitions JOIN pg_catalog.pg_class leaf ON leaf.oid=partitions.id WHERE leaf.relkind='f'))`
	return "LOCK TABLE ONLY " + relation + " IN ROW EXCLUSIVE MODE;\n" + requireCatalogRow(localTable) + `
SELECT pg_catalog.set_config('pgs3.saved_relation_kinds',pg_catalog.current_setting('restrict_nonsystem_relation_kind'),true);
SELECT pg_catalog.set_config('restrict_nonsystem_relation_kind',
 pg_catalog.concat_ws(',',NULLIF(pg_catalog.current_setting('restrict_nonsystem_relation_kind'),''),'foreign-table'),true);
`
}

const endExtensionRows = "SELECT pg_catalog.set_config('restrict_nonsystem_relation_kind',pg_catalog.current_setting('pgs3.saved_relation_kinds'),true);"

type tocEdit struct {
	at      int64
	replace int
	data    []byte
}

func prepareGuardedRestore(ctx context.Context, src, dst *os.File, m archiveMetadata) error {
	l := m.layout
	extra := int64(len(m.extensionTables))
	for _, t := range m.extensionTables {
		if t.kind != "SEQUENCE" {
			extra++
		}
	}
	if l == nil || len(l.spans) != len(m.extensionTables) || l.maxID+extra > math.MaxInt32 || l.count+extra > math.MaxInt32 {
		return fmt.Errorf("cannot add extension data checks to archive TOC")
	}
	edits := []tocEdit{{l.countAt, l.intSize + 1, archiveInt(l.count+extra, l.intSize)}}
	var growth uint64
	id := l.maxID
	for i, span := range l.spans {
		before := l.sqlEntry(id+1, beginExtensionRows(m.extensionTables[i]))
		if m.extensionTables[i].kind == "SEQUENCE" {
			edits = append(edits, tocEdit{span.start, 0, before})
			growth += uint64(len(before))
			id++
			continue
		}
		after := l.sqlEntry(id+2, endExtensionRows)
		edits = append(edits, tocEdit{span.start, 0, before}, tocEdit{span.end, 0, after})
		growth += uint64(len(before) + len(after))
		id += 2
	}
	for _, p := range m.proofs {
		name, err := decodeCatalogText(p.Name)
		if err != nil {
			return err
		}
		span, ok := l.extensionSQL[name]
		if !ok || span.end-span.start < int64(l.intSize+1) || span.end-span.start > 1<<20 {
			return fmt.Errorf("invalid EXTENSION definition")
		}
		old := make([]byte, span.end-span.start)
		if _, err := src.ReadAt(old, span.start); err != nil {
			return err
		}
		definition := string(old[l.intSize+1:])
		sql := strings.TrimRight(definition, " \t\r\n")
		if !strings.HasPrefix(sql, "CREATE EXTENSION ") || !strings.HasSuffix(sql, ";") {
			return fmt.Errorf("unrecognized EXTENSION definition")
		}
		version, err := decodeCatalogText(p.EncodedVersion)
		if err != nil {
			return err
		}
		tag := "$pgs3version$"
		for strings.Contains(version, tag) {
			tag = "$v" + tag[1:]
		}
		sql = strings.TrimSuffix(sql, ";") + " VERSION " + tag + version + tag + definition[len(sql)-1:] + "\n"
		// IF NOT EXISTS can keep an already installed extension. Check its
		// actual version in this same pg_restore transaction.
		sql += requireCatalogRow(fmt.Sprintf("SELECT FROM pg_catalog.pg_extension WHERE pg_catalog.encode(pg_catalog.convert_to(extname::text,pg_catalog.current_setting('client_encoding')::pg_catalog.name),'hex')='%s' AND pg_catalog.encode(pg_catalog.convert_to(extversion,'UTF8'),'hex')='%x'", p.Name, p.Version))
		data := append(archiveInt(int64(len(sql)), l.intSize), []byte(sql)...)
		edits = append(edits, tocEdit{span.start, len(old), data})
		growth += uint64(len(data) - len(old))
	}
	for _, off := range l.offsets {
		adjusted := off.value + growth
		if adjusted < off.value || (l.offsetSize == 4 && adjusted > math.MaxUint32) {
			return fmt.Errorf("archive data offset overflow")
		}
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, adjusted)
		edits = append(edits, tocEdit{off.at, l.offsetSize, b[:l.offsetSize]})
	}
	sort.SliceStable(edits, func(i, j int) bool { return edits[i].at < edits[j].at })
	if _, err := src.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var pos int64
	for _, e := range edits {
		if err := ctx.Err(); err != nil {
			return err
		}
		if e.at < pos {
			return fmt.Errorf("overlapping archive TOC entries")
		}
		if _, err := io.CopyN(dst, src, e.at-pos); err != nil {
			return err
		}
		if _, err := dst.Write(e.data); err != nil {
			return err
		}
		if e.replace > 0 {
			if _, err := src.Seek(int64(e.replace), io.SeekCurrent); err != nil {
				return err
			}
		}
		pos = e.at + int64(e.replace)
	}
	// Check cancellation between large sequential copies even on slow volumes.
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err := io.CopyN(dst, src, 8<<20)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
}

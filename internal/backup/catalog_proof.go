package backup

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
)

// The custom archive's data blocks keep their native offsets. A length and
// signature at EOF locate our catalog metadata without scanning row data.
// The trailer is inside GPG encryption and is removed from the restore copy.
const catalogSignature = "PGS3EXT1"
const maxCatalogSize = 64 << 20

type extensionProof struct {
	Name, Version, EncodedVersion string
	Members                       []configMember
}

type configMember struct {
	Schema, Name, OID, Kind string
	Local                   bool
}

func decodeCatalogText(value string) (string, error) {
	b, err := hex.DecodeString(value)
	if err != nil || len(b) == 0 || strings.IndexByte(string(b), 0) >= 0 {
		return "", fmt.Errorf("invalid archive catalog string")
	}
	return string(b), nil
}

func verifyCatalog(ctx context.Context, snap *catalogSnapshot, m *archiveMetadata) error {
	if len(m.extensions) == 0 && len(m.emptyMappings) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if snap == nil {
		return fmt.Errorf("extension or user mapping appeared after source inspection; retry backup")
	}
	if m.encoding == "" {
		return fmt.Errorf("missing archive encoding for catalog checks")
	}
	// Use hex transport, including for the psql variable assignment.
	setEncoding := fmt.Sprintf("SELECT convert_from(decode('%x','hex'),'UTF8') AS archive_encoding \\gset\n", m.encoding)
	var active bool
	if err := snap.query(setEncoding+"SELECT to_json(true);", &active); err != nil {
		return err
	}
	if len(m.emptyMappings) > 0 {
		if err := checkEmptyMappings(snap, *m); err != nil {
			return err
		}
	}
	if len(m.extensions) == 0 {
		return nil
	}
	names := make([]string, len(m.extensions))
	for i, name := range m.extensions {
		names[i] = archiveNameSQL(name)
	}
	query := `SELECT coalesce(json_agg(json_build_object(
 'Name',encode(convert_to(e.extname::text,:'archive_encoding'),'hex'),
 'Version',e.extversion,'EncodedVersion',encode(convert_to(e.extversion,:'archive_encoding'),'hex'),
 'Members',(SELECT json_agg(json_build_object(
   'Schema',encode(convert_to(n.nspname::text,:'archive_encoding'),'hex'),
   'Name',encode(convert_to(c.relname::text,:'archive_encoding'),'hex'),
   'OID',c.oid::text,'Kind',c.relkind::text,
   'Local',c.relkind IN ('r','p','S') AND (c.relkind <> 'p' OR NOT EXISTS (
     WITH RECURSIVE parts(id) AS (SELECT c.oid UNION SELECT inhrelid FROM pg_inherits JOIN parts ON inhparent=id)
     SELECT FROM parts JOIN pg_class leaf ON leaf.oid=parts.id WHERE leaf.relkind='f'))))
   FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
   JOIN pg_depend d ON d.classid='pg_class'::regclass AND d.objid=c.oid AND d.objsubid=0
     AND d.refclassid='pg_extension'::regclass AND d.refobjid=e.oid AND d.deptype='e'
   WHERE c.oid=ANY(e.extconfig))
 )), '[]'::json) FROM pg_extension e WHERE e.extname::text IN (` + strings.Join(names, ",") + ");"
	if err := snap.query(query, &m.proofs); err != nil {
		return err
	}
	return matchCatalog(m)
}

func writeCatalogProof(w io.Writer, proofs []extensionProof) error {
	b, err := json.Marshal(proofs)
	if err != nil {
		return err
	}
	if len(b) > maxCatalogSize {
		return fmt.Errorf("extension metadata exceeds %d bytes", maxCatalogSize)
	}
	if _, err = w.Write(b); err != nil {
		return err
	}
	var tail [8 + len(catalogSignature)]byte
	binary.LittleEndian.PutUint64(tail[:8], uint64(len(b)))
	copy(tail[8:], catalogSignature)
	_, err = w.Write(tail[:])
	return err
}

func readCatalogProof(f *os.File, m *archiveMetadata) error {
	if len(m.extensions) == 0 {
		return nil
	}
	info, err := f.Stat()
	if err != nil {
		return err
	}
	var tail [8 + len(catalogSignature)]byte
	end := info.Size() - int64(len(tail))
	if end < 0 {
		return fmt.Errorf("missing extension version metadata")
	}
	if _, err = f.ReadAt(tail[:], end); err != nil {
		return err
	}
	if string(tail[8:]) != catalogSignature {
		return fmt.Errorf("archive lacks verified extension version metadata")
	}
	n := binary.LittleEndian.Uint64(tail[:8])
	if n == 0 || n > uint64(end) || n > maxCatalogSize {
		return fmt.Errorf("invalid extension metadata length")
	}
	raw := make([]byte, int(n))
	if _, err = f.ReadAt(raw, end-int64(n)); err != nil {
		return err
	}
	if err = json.Unmarshal(raw, &m.proofs); err != nil {
		return fmt.Errorf("invalid extension metadata")
	}
	if err := matchCatalog(m); err != nil {
		return err
	}
	return f.Truncate(end - int64(n))
}

func matchCatalog(m *archiveMetadata) error {
	type identity struct{ schema, name string }
	type owner struct {
		name   string
		member configMember
	}
	owners := make(map[identity]owner)
	expected := make(map[string]bool)
	for _, e := range m.extensions {
		expected[e] = true
	}
	for _, p := range m.proofs {
		name, err := decodeCatalogText(p.Name)
		if err != nil || !expected[name] || p.Version == "" {
			return fmt.Errorf("unexpected or duplicate extension metadata")
		}
		if _, err := decodeCatalogText(p.EncodedVersion); err != nil {
			return err
		}
		delete(expected, name)
		for _, member := range p.Members {
			ns, err := decodeCatalogText(member.Schema)
			if err != nil {
				return err
			}
			rel, err := decodeCatalogText(member.Name)
			if err != nil {
				return err
			}
			key := identity{ns, rel}
			if _, found := owners[key]; found {
				return fmt.Errorf("ambiguous extension configuration relation")
			}
			owners[key] = owner{name, member}
		}
	}
	if len(expected) != 0 {
		return fmt.Errorf("incomplete extension version metadata")
	}
	for i := range m.extensionTables {
		t := &m.extensionTables[i]
		o, ok := owners[identity{t.namespace, t.name}]
		validKind := o.member.Kind == "S" && t.kind == "SEQUENCE" ||
			(o.member.Kind == "r" || o.member.Kind == "p") && t.kind == "TABLE" && o.member.OID == t.oid
		if !ok || !validKind || !o.member.Local {
			return fmt.Errorf("WARNING: %q.%q is not a verified local configuration relation of an archived extension", t.namespace, t.name)
		}
		t.owner = o.name
	}
	return nil
}

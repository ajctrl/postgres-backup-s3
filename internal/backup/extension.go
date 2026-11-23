package backup

import (
	"context"
	"fmt"
	"strings"
)

// Reject conflicting objects already present before pg_restore. The SQL in
// the adjusted TOC repeats the identity check after CREATE EXTENSION.
func checkTargetRelations(ctx context.Context, runner commandRunner, conn []string, m archiveMetadata) error {
	if len(m.extensionTables) == 0 {
		return nil
	}
	var values []string
	for _, t := range m.extensionTables {
		values = append(values, fmt.Sprintf("(%s,%s,%s,%t)", archiveNameSQL(t.namespace), archiveNameSQL(t.name), archiveNameSQL(t.owner), t.kind == "SEQUENCE"))
	}
	query := "WITH expected(ns, rel, owner, seq) AS (VALUES " + strings.Join(values, ",") + `) SELECT to_json(NOT EXISTS (
 SELECT FROM expected JOIN pg_namespace n ON n.nspname::text=ns
 JOIN pg_class c ON c.relnamespace=n.oid AND c.relname::text=rel
 WHERE (CASE WHEN seq THEN c.relkind <> 'S' ELSE c.relkind NOT IN ('r','p') END)
 OR NOT EXISTS (SELECT FROM pg_extension e WHERE e.extname::text=owner AND c.oid=ANY(e.extconfig))
 ));`
	var ok bool
	args := append([]string{"-XAt", "-v", "ON_ERROR_STOP=1", "-v", "archive_encoding=" + m.encoding, "-f", "-"}, conn...)
	if err := runner.readJSON(ctx, strings.NewReader(query), &ok, args...); err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("target relation is not a verified local configuration relation of its archived extension")
	}
	return nil
}

func archiveNameSQL(name string) string {
	return fmt.Sprintf("convert_from(decode('%x','hex'), :'archive_encoding')", name)
}

// The archive has no catalog OID for USER MAPPING. Match its pg_dump tag,
// rejecting ambiguous tags. Read only the nullness of the catalog column.
func checkEmptyMappings(snapshot *catalogSnapshot, m archiveMetadata) error {
	var query strings.Builder
	query.WriteString(`SELECT has_column_privilege('pg_catalog.pg_user_mapping','oid','SELECT')
 AND has_column_privilege('pg_catalog.pg_user_mapping','umoptions','SELECT') AS raw_catalog \gset
`)
	query.WriteString("WITH expected(tag) AS (VALUES ")
	for i, tag := range m.emptyMappings {
		if i > 0 {
			query.WriteByte(',')
		}
		fmt.Fprintf(&query, "(convert_from(decode('%x','hex'), :'archive_encoding'))", tag)
	}
	query.WriteString(`), mappings AS (
\if :raw_catalog
 SELECT v.usename, v.srvname, coalesce(cardinality(m.umoptions),0)=0 AS empty
 FROM pg_user_mappings v JOIN pg_user_mapping m ON m.oid=v.umid
\else
 SELECT v.usename, v.srvname,
   ((v.umuser<>0 AND v.usename=current_user AND
      (pg_has_role(s.srvowner,'USAGE') OR has_server_privilege(s.oid,'USAGE')))
    OR (v.umuser=0 AND pg_has_role(s.srvowner,'USAGE')))
   AND coalesce(cardinality(v.umoptions),0)=0 AS empty
 FROM pg_user_mappings v JOIN pg_foreign_server s ON s.oid=v.srvid
\endif
), verified AS (
 SELECT 'USER MAPPING ' || usename || ' SERVER ' || srvname AS tag FROM mappings
 GROUP BY 1 HAVING count(*) = 1 AND bool_and(empty)
) SELECT to_json(NOT EXISTS (SELECT tag FROM expected EXCEPT SELECT tag FROM verified));`)
	var verified bool
	if err := snapshot.query(query.String(), &verified); err != nil {
		return fmt.Errorf("USER MAPPING has no visible options and catalog verification failed: %w", err)
	}
	// The login may have direct catalog visibility even when the dump role
	// does not. If its view is insufficient, try the role used by pg_dump.
	if !verified && snapshot.role != "" {
		sql := "SAVEPOINT mapping_role; SET LOCAL ROLE :\"dump_role\";\n" + query.String()
		if err := snapshot.query(sql, &verified); err != nil {
			return fmt.Errorf("verify USER MAPPING using dump role: %w", err)
		}
		if verified {
			var restored bool
			if err := snapshot.query("ROLLBACK TO mapping_role; RELEASE mapping_role; SELECT to_json(true);", &restored); err != nil {
				return err
			}
		}
	}
	if !verified {
		return fmt.Errorf("USER MAPPING has no visible options and could not be verified as empty")
	}
	return nil
}

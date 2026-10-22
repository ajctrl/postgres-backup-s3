package backup

import (
	"fmt"
	"strings"
)

type dumpOptions struct {
	args           []string
	partial        bool
	minClientMajor int
}

// Only complete, known option names are accepted. A denylist would miss getopt
// abbreviations, bundled short options, and positional connection strings.
func parseDumpOptions(value string, allowPartial bool) (*dumpOptions, error) {
	args := strings.FieldsFunc(value, func(r rune) bool { return r == ' ' || r == '\t' || r == '\n' })
	result := &dumpOptions{args: args}
	flags := strings.Fields("blobs large-objects no-blobs no-large-objects schema-only no-privileges no-acl verbose no-password column-inserts disable-dollar-quoting enable-row-security inserts load-via-partition-root no-comments no-publications no-security-labels no-subscriptions no-toast-compression no-unlogged-table-data on-conflict-do-nothing quote-all-identifiers serializable-deferrable strict-names no-statistics")
	values := strings.Fields("compress encoding exclude-schema exclude-table exclude-table-data exclude-table-and-children exclude-table-data-and-children extension exclude-extension filter lock-wait-timeout role rows-per-insert schema section snapshot table table-and-children")
	allowed := make(map[string]bool, len(flags)+len(values))
	for _, flag := range flags {
		allowed[flag] = false
	}
	for _, option := range values {
		allowed[option] = true
	}
	sections, preData := false, false
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if strings.HasPrefix(arg, "--") {
			name, value, hasValue := strings.Cut(arg[2:], "=")
			if name == "data-only" || name == "statistics-only" {
				return nil, fmt.Errorf("PGDUMP_EXTRA_OPTS: --%s is not supported; backups must include object definitions", name)
			}
			switch name {
			case "no-synchronized-snapshots":
				return nil, fmt.Errorf("PGDUMP_EXTRA_OPTS: --no-synchronized-snapshots is not supported by PostgreSQL 17 or 18")
			case "extra-float-digits":
				return nil, fmt.Errorf("PGDUMP_EXTRA_OPTS: --extra-float-digits is not supported; backups must preserve floating-point precision")
			case "binary-upgrade":
				return nil, fmt.Errorf("PGDUMP_EXTRA_OPTS: --binary-upgrade is for upgrade utilities and cannot be used for normal backup/restore")
			case "no-owner", "no-tablespaces", "no-table-access-method":
				setting := "RESTORE_" + strings.ToUpper(strings.ReplaceAll(name, "-", "_"))
				return nil, fmt.Errorf("PGDUMP_EXTRA_OPTS: --%s has no effect on custom archives; use %s=true when restoring", name, setting)
			case "clean", "create", "if-exists", "disable-triggers", "use-set-session-authorization":
				return nil, fmt.Errorf("PGDUMP_EXTRA_OPTS: --%s has no effect on custom archives; restore behavior is managed by this application", name)
			}
			needsValue, ok := allowed[name]
			if !ok || (hasValue && !needsValue) {
				return nil, fmt.Errorf("PGDUMP_EXTRA_OPTS: unsupported option --%s; output, format and connection options are managed by this application", name)
			}
			if needsValue && !hasValue {
				i++
				if i == len(args) {
					return nil, fmt.Errorf("PGDUMP_EXTRA_OPTS: --%s requires a value", name)
				}
				value = args[i]
			}
			if name == "section" {
				sections = true
				preData = preData || value == "pre-data"
			}
			if name == "filter" && (value == "-" || value == "") {
				return nil, fmt.Errorf("PGDUMP_EXTRA_OPTS: --filter requires a file path; standard input (-) is not connected")
			}
			if name == "no-statistics" {
				result.minClientMajor = 18
			}
			switch name {
			case "table", "table-and-children", "schema", "extension", "filter", "section",
				"exclude-schema", "exclude-table", "exclude-table-data", "exclude-table-and-children",
				"exclude-table-data-and-children", "exclude-extension", "schema-only", "enable-row-security",
				"no-blobs", "no-large-objects", "no-unlogged-table-data", "no-privileges", "no-acl",
				"no-comments", "no-publications", "no-security-labels", "no-subscriptions":
				result.partial = true
			}
			continue
		}
		if !strings.HasPrefix(arg, "-") || arg == "-" {
			return nil, fmt.Errorf("PGDUMP_EXTRA_OPTS: positional arguments are not allowed; select the database using POSTGRES_DATABASE or POSTGRES_DATABASES")
		}
		for j := 1; j < len(arg); j++ {
			option := arg[j]
			if option == 'a' {
				return nil, fmt.Errorf("PGDUMP_EXTRA_OPTS: -a is not supported; backups must include object definitions")
			}
			if option == 'O' {
				return nil, fmt.Errorf("PGDUMP_EXTRA_OPTS: -O has no effect on custom archives; use RESTORE_NO_OWNER=true when restoring")
			}
			if option == 'c' || option == 'C' {
				return nil, fmt.Errorf("PGDUMP_EXTRA_OPTS: -%c has no effect on custom archives; restore behavior is managed by this application", option)
			}
			if strings.ContainsRune("sBxenNtT", rune(option)) {
				result.partial = true
			}
			if strings.ContainsRune("bsBvxw", rune(option)) {
				continue
			}
			if !strings.ContainsRune("eEnNtTZ", rune(option)) {
				return nil, fmt.Errorf("PGDUMP_EXTRA_OPTS: unsupported option -%c; output, format and connection options are managed by this application", option)
			}
			if j == len(arg)-1 {
				i++
				if i == len(args) {
					return nil, fmt.Errorf("PGDUMP_EXTRA_OPTS: -%c requires a value", option)
				}
			}
			break // An option requiring a value consumes the rest of this token.
		}
	}
	if sections && !preData {
		return nil, fmt.Errorf("PGDUMP_EXTRA_OPTS: --section selections must include pre-data; data-only backups are not supported")
	}
	if result.partial && !allowPartial {
		return nil, fmt.Errorf("PGDUMP_EXTRA_OPTS: partial database contents require BACKUP_ALLOW_PARTIAL=true; database-level selection does not require this setting")
	}
	return result, nil
}

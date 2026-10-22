package backup

import (
	"reflect"
	"strings"
	"testing"
)

func TestDumpOptionsRejectOverrides(t *testing.T) {
	for _, option := range []string{
		"--file=/tmp/out", "--file /tmp/out", "--fil=/tmp/out", "--format=plain", "--format plain", "--fo=plain",
		"-f/tmp/out", "-f /tmp/out", "-Fplain", "-vFplain", "-vf/tmp/out", "-vOf/tmp/out",
		"--dbname=other", "--db=other", "-dother", "-vdother", "other", "postgresql:///other", "-- other",
		"--host=other", "-hother", "--port=1234", "-p1234", "--username=other", "-Uother",
		"--help", "--version", "-V", "--no-owner=true", "--compress", "-Z", "--unknown", "--", "-",
		"--data-only", "-a", "-va", "--statistics-only", "--section=data", "--section data --section post-data",
		"--no-owner", "-O", "-vO",
		"--extra-float-digits=-15", "--extra-float-digits -2", "--extra-float-digits=3", "--extra-float-digits",
		"--binary-upgrade", "--no-tablespaces", "--no-table-access-method",
		"--clean", "--create", "--if-exists", "--clean --if-exists", "--disable-triggers", "--use-set-session-authorization",
		"-c", "-C", "-vc", "-vC", "-cC", "--no-tablespace", "--extra-float-digi=-2",
		"--filter=-", "--filter -", "--filter=", "--no-synchronized-snapshots",
	} {
		t.Run(option, func(t *testing.T) {
			for _, allowPartial := range []bool{false, true} {
				if _, err := parseDumpOptions(option, allowPartial); err == nil {
					t.Fatalf("accepted unsafe/invalid extra option %q (partial=%v)", option, allowPartial)
				}
			}
		})
	}
}

func TestDumpOptionsPreserveSafeArguments(t *testing.T) {
	for _, option := range []string{
		"", "--compress=0 --no-acl --exclude-table=*.tmp", "--compress zstd:level=3 -vx -Z0",
		"-t public.* -N scratch -n public --section=pre-data --section data", "--exclude-table-data audit --schema-only",
		"--section pre-data", "-s",
		"--exclude-table=--file=literal", "-t--host=literal", "-vZ0",
		"--exclude-table=--no-tablespaces", "-t--create", "--filter=selection.txt",
	} {
		got, err := parseDumpOptions(option, true)
		if err != nil || !reflect.DeepEqual(got.args, strings.Fields(option)) {
			t.Fatalf("safe arguments changed: %q -> %v (%v)", option, got, err)
		}
	}
}

func TestDumpOptionsExplainRestoreSettings(t *testing.T) {
	for option, setting := range map[string]string{
		"--no-owner": "RESTORE_NO_OWNER", "-O": "RESTORE_NO_OWNER",
		"--no-tablespaces":         "RESTORE_NO_TABLESPACES",
		"--no-table-access-method": "RESTORE_NO_TABLE_ACCESS_METHOD",
	} {
		if _, err := parseDumpOptions(option, false); err == nil || !strings.Contains(err.Error(), setting+"=true") {
			t.Errorf("%s should explain %s: %v", option, setting, err)
		}
	}
}

func TestPartialDumpOptionsRequireExplicitPermission(t *testing.T) {
	for _, option := range []string{
		"--table=orders", "--table-and-children=p", "--schema=public", "--extension=hstore", "--filter=selection.txt",
		"--section=pre-data --section=data", "--schema-only", "--enable-row-security", "--no-blobs", "--no-large-objects",
		"--no-unlogged-table-data", "--exclude-schema=scratch", "--exclude-table=temp", "--exclude-table-data=logs",
		"--exclude-table-and-children=p", "--exclude-table-data-and-children=p", "--exclude-extension=hstore",
		"--no-acl", "--no-privileges", "--no-comments", "--no-publications", "--no-security-labels", "--no-subscriptions",
		"-s", "-B", "-vx", "-e hstore", "-n public", "-N scratch", "-torders", "-Ttemp", "-vsB",
	} {
		t.Run(option, func(t *testing.T) {
			if _, err := parseDumpOptions(option, false); err == nil || !strings.Contains(err.Error(), "BACKUP_ALLOW_PARTIAL=true") {
				t.Fatalf("partial option did not require permission: %v", err)
			}
			got, err := parseDumpOptions(option, true)
			if err != nil || !got.partial || !reflect.DeepEqual(got.args, strings.Fields(option)) {
				t.Fatalf("authorized partial option changed: %#v, %v", got, err)
			}
		})
	}
	for _, option := range []string{"", "--compress=0", "-vZ0", "--encoding=UTF8", "-E UTF8", "--blobs", "-b", "--inserts", "--no-statistics", "-Z--schema-only"} {
		got, err := parseDumpOptions(option, false)
		if err != nil || got.partial {
			t.Errorf("non-selection option classified as partial: %s: %#v, %v", option, got, err)
		}
	}
}

func TestDumpClientRequirementTracksOptionsNotValues(t *testing.T) {
	for _, tc := range []struct {
		options string
		minimum int
	}{
		{"--no-statistics", 18}, {"--no-statistics --no-statistics", 18}, {"--compress=0", 0},
		{"--table=--no-statistics", 0}, {"--table --no-statistics", 0}, {"-t--no-statistics", 0},
	} {
		got, err := parseDumpOptions(tc.options, true)
		if err != nil || got.minClientMajor != tc.minimum {
			t.Errorf("%s: %#v, %v", tc.options, got, err)
		}
	}
}

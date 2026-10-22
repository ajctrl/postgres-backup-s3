package backup

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func postgresTestUnavailable(t *testing.T, reason string) {
	t.Helper()
	if os.Getenv("PG_TEST_REQUIRED") == "true" {
		t.Fatalf("required PostgreSQL test cannot run: %s", reason)
	}
	t.Skip(reason)
}

func postgresTestBin(t *testing.T) string {
	t.Helper()
	switch os.Getenv("PG_TEST_REQUIRED") {
	case "", "false", "true":
	default:
		t.Fatal("PG_TEST_REQUIRED must be true or false")
	}
	bin := os.Getenv("PG_TEST_BINDIR")
	if bin == "" {
		path, err := exec.LookPath("initdb")
		if err != nil {
			postgresTestUnavailable(t, "PostgreSQL server tools unavailable; set PG_TEST_BINDIR")
		}
		bin = filepath.Dir(path)
	}
	if os.Geteuid() == 0 {
		postgresTestUnavailable(t, "initdb requires an unprivileged user")
	}
	return bin
}

func TestRequiredPostgreSQLTestsFailWithoutTools(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	// Run only the native tests in a child, with no discoverable tools.
	cmd := exec.CommandContext(t.Context(), executable, "-test.run=Test(ArchiveSchema|RestorePlacement)WithPostgreSQL")
	cmd.Env = append(withoutEnv(withoutEnv(withoutEnv(os.Environ(), "PATH"), "PG_TEST_BINDIR"), "PG_TEST_REQUIRED"),
		"PATH="+t.TempDir(), "PG_TEST_REQUIRED=true")
	output, err := cmd.CombinedOutput()
	if err == nil || bytes.Count(output, []byte("required PostgreSQL test cannot run")) != 2 {
		t.Fatalf("required tests must fail for missing tools: %v\n%s", err, output)
	}
}

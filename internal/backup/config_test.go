package backup

import (
	"strings"
	"testing"
	"time"
)

func TestConfigDefaultsAndValidation(t *testing.T) {
	base := map[string]string{"S3_BUCKET": "bucket", "POSTGRES_USER": "user", "POSTGRES_PASSWORD": "secret", "POSTGRES_HOST": "db"}
	load := func(extra map[string]string) (Config, error) {
		return loadConfig(func(key string) (string, bool) {
			if value, ok := extra[key]; ok {
				return value, true
			}
			value, ok := base[key]
			return value, ok
		})
	}
	c, err := load(nil)
	if err != nil || c.Prefix != "backup" || c.Port != "5432" || c.FilenameMode != "timestamp" || c.Storage.Region != "us-west-1" {
		t.Fatalf("default configuration: %#v, %v", c, err)
	}
	c, err = load(map[string]string{"S3_PREFIX": "", "POSTGRES_HOST": "", "POSTGRES_PORT_5432_TCP_ADDR": "legacy", "POSTGRES_PORT_5432_TCP_PORT": "5433"})
	if err != nil || c.Prefix != "" || c.Host != "legacy" || c.Port != "5433" {
		t.Fatalf("legacy/default overrides: %#v, %v", c, err)
	}
	for _, key := range []string{"S3_BUCKET", "POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_HOST"} {
		if _, err := load(map[string]string{key: ""}); err == nil || !strings.Contains(err.Error(), key) {
			t.Errorf("empty %s: %v", key, err)
		}
	}
	if _, err := load(map[string]string{"BACKUP_FILENAME_MODE": "wrong"}); err == nil {
		t.Fatal("accepted unknown filename mode")
	}
}

func TestSelectionAndRetentionValidation(t *testing.T) {
	valid := Config{Database: "app", BackupAll: "false"}
	for _, days := range []string{"", "1", "7", "36500"} {
		c := valid
		c.KeepDays = days
		if _, err := c.validateBackup(); err != nil {
			t.Errorf("valid retention %q: %v", days, err)
		}
	}
	for _, days := range []string{"0", "01", "-1", "1.0", "+1", " 1", "1\n", "abc", "36501", "999999999999999999999", "１２"} {
		c := valid
		c.KeepDays = days
		if _, err := c.validateBackup(); err == nil {
			t.Errorf("accepted invalid retention %q", days)
		}
	}
	for _, c := range []Config{
		{BackupAll: "false"}, {BackupAll: "True"}, {Database: "app", BackupAll: "true"},
		{Databases: "app,billing", BackupAll: "true"}, {Database: "app", BackupAll: "false", Exclude: "scratch"},
	} {
		if _, err := c.validateBackup(); err == nil {
			t.Errorf("accepted invalid selection %#v", c)
		}
	}
}

func TestNamesAndKeysPreserveExistingLayout(t *testing.T) {
	for name, encoded := range map[string]string{
		"a/b": "a%2Fb", "a%2Fb": "a%252Fb", "a'b": "a%27b", "with space": "with%20space",
		"host=other": "host%3Dother", "$(oops)*": "%24%28oops%29%2A", "日本語": "%E6%97%A5%E6%9C%AC%E8%AA%9E",
	} {
		if got := databaseURI(name); got != "postgresql:///"+encoded {
			t.Errorf("databaseURI(%q) = %q", name, got)
		}
	}
	for _, tc := range []struct{ prefix, timestampPrefix, fixedPrefix string }{
		{"backup", "backup/", "backup/"}, {"backup/", "backup//", "backup/"},
		{"backup//", "backup///", "backup//"}, {"", "/", ""}, {"/", "//", ""},
	} {
		c := Config{Prefix: tc.prefix, FilenameMode: "timestamp"}
		date := time.Date(2026, 9, 12, 12, 30, 0, 0, time.FixedZone("plus2", 2*3600))
		if got := c.backupKey("app", date); got != tc.timestampPrefix+"app_2026-09-12T10:30:00.dump" {
			t.Errorf("timestamp prefix %q: %q", tc.prefix, got)
		}
		c.FilenameMode, c.Passphrase = "fixed", "secret"
		for name, directory := range map[string]string{"app": "app", "a/b": "a%2Fb", ".": "%2E", "..": "%2E%2E"} {
			if got := c.backupKey(name, date); got != tc.fixedPrefix+directory+"/latest.dump.gpg" {
				t.Errorf("fixed key %q/%q: %q", tc.prefix, name, got)
			}
		}
	}
	for _, names := range [][]string{nil, {""}, {"app", ""}, {"a\nb"}, {"a\x00b"}, {"a\x7fb"}, {string([]byte{0xff})}} {
		if err := validateDatabases(names); err == nil {
			t.Errorf("accepted invalid names %q", names)
		}
	}
}

func TestScheduleCompatibility(t *testing.T) {
	for _, spec := range []string{"@weekly", "@daily", "@every 1h", "0 2 * * *", "0 0 2 * * *", "CRON_TZ=Asia/Tokyo 0 2 * * *"} {
		schedule, err := scheduleParser().Parse(spec)
		if err != nil || schedule.Next(time.Now()).IsZero() {
			t.Errorf("invalid schedule %q: %v", spec, err)
		}
	}
	for _, spec := range []string{"", "garbage", "* *", "61 * * * * *"} {
		if _, err := scheduleParser().Parse(spec); err == nil {
			t.Errorf("accepted invalid schedule %q", spec)
		}
	}
}

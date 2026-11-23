package backup

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

func TestConfigDefaultsAndValidation(t *testing.T) {
	base := map[string]string{"S3_BUCKET": "bucket", "POSTGRES_USER": "user", "POSTGRES_PASSWORD": "secret", "POSTGRES_HOST": "db"}
	load := func(extra map[string]string) (Config, error) {
		command := "backup"
		if _, ok := extra["RESTORE_TIMEOUT"]; ok {
			command = "restore"
		}
		return loadConfig(command, func(key string) (string, bool) {
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
	c, err = load(map[string]string{"BACKUP_DATABASE_TIMEOUT": "90m"})
	if err != nil || c.DatabaseTimeout != 90*time.Minute {
		t.Fatalf("database timeout: %s, %v", c.DatabaseTimeout, err)
	}
	if _, err := load(map[string]string{"S3_UPLOAD_PART_SIZE_MB": "5120", "S3_UPLOAD_BUFFER_BUDGET_MB": "30720"}); err != nil {
		t.Fatal(err)
	}
	c, err = load(map[string]string{"RESTORE_TIMEOUT": "30m"})
	if err != nil || c.RestoreTimeout != 30*time.Minute {
		t.Fatalf("restore timeout: %v %v", c.RestoreTimeout, err)
	}
	for _, value := range []string{"0", "-1s", "60", "invalid"} {
		if _, err := load(map[string]string{"RESTORE_TIMEOUT": value}); err == nil {
			t.Errorf("accepted invalid restore timeout %q", value)
		}
		if _, err := load(map[string]string{"BACKUP_DATABASE_TIMEOUT": value}); err == nil {
			t.Errorf("accepted invalid database timeout %q", value)
		}
	}
}

func TestCommandSpecificSettings(t *testing.T) {
	for _, tc := range []struct{ cmd, key string }{
		{"backup", "RESTORE_TIMEOUT"}, {"run", "RESTORE_NO_OWNER"}, {"restore", "S3_UPLOAD_PART_SIZE_MB"}, {"restore", "BACKUP_ALLOW_PARTIAL"}, {"restore", "BACKUP_DATABASE_TIMEOUT"}, {"restore", "BACKUP_ALLOW_UNVERSIONED_OVERWRITE"},
	} {
		env := map[string]string{"S3_BUCKET": "bucket", "POSTGRES_USER": "user", "POSTGRES_PASSWORD": "secret", "POSTGRES_HOST": "host", tc.key: "invalid"}
		if _, err := loadConfig(tc.cmd, func(k string) (string, bool) { v, ok := env[k]; return v, ok }); err != nil {
			t.Errorf("%s considered %s: %v", tc.cmd, tc.key, err)
		}
	}
}

func TestSelectionAndRetentionValidation(t *testing.T) {
	valid := Config{Database: "app", BackupAll: "false"}
	for _, days := range []string{"", "1", "7", "36500"} {
		c := valid
		c.KeepDays = days
		if _, _, err := c.validateBackup(); err != nil {
			t.Errorf("valid retention %q: %v", days, err)
		}
	}
	for _, days := range []string{"0", "01", "-1", "1.0", "+1", " 1", "1\n", "abc", "36501", "999999999999999999999", "１２"} {
		c := valid
		c.KeepDays = days
		if _, _, err := c.validateBackup(); err == nil {
			t.Errorf("accepted invalid retention %q", days)
		}
	}
	for _, c := range []Config{
		{BackupAll: "false"}, {BackupAll: "True"}, {Database: "app", BackupAll: "true"},
		{Databases: "app,billing", BackupAll: "true"}, {Database: "app", BackupAll: "false", Exclude: "scratch"},
	} {
		if _, _, err := c.validateBackup(); err == nil {
			t.Errorf("accepted invalid selection %#v", c)
		}
	}
}

func TestNamesAndKeys(t *testing.T) {
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
		date := time.Date(2026, 9, 12, 12, 30, 0, 0, time.FixedZone("plus2", 2*3600))
		for name, component := range map[string]string{"app": "app", "a/b": "a%2Fb", "a%2Fb": "a%252Fb", ".": "%2E", "..": "%2E%2E"} {
			for _, passphrase := range []string{"", "secret"} {
				c := Config{Prefix: tc.prefix, FilenameMode: "timestamp", Passphrase: passphrase}
				suffix := ".dump"
				if passphrase != "" {
					suffix += ".gpg"
				}
				got, err := c.backupKey(name, date)
				if err != nil || !strings.HasPrefix(got, tc.timestampPrefix+"v2/full/"+component+"/2026-09-12T10:30:00-") || !strings.HasSuffix(got, suffix) || !c.isTimestampBackup(name, got) {
					t.Errorf("generation key %q/%q: %q, %v", tc.prefix, name, got, err)
				}
				second, err := c.backupKey(name, date)
				if err != nil || second == got {
					t.Fatalf("same timestamp reused a generation: %q, %v", second, err)
				}
				c.FilenameMode = "fixed"
				if got, err := c.backupKey(name, date); err != nil || got != tc.fixedPrefix+"v2/full/"+component+suffix {
					t.Errorf("fixed key %q/%q: %q, %v", tc.prefix, name, got, err)
				}
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

func TestIncompleteScheduleTimezoneIsRejected(t *testing.T) {
	for _, spec := range []string{"TZ=", "TZ=Asia/Tokyo", "CRON_TZ=UTC ", "TZ= @daily", "@every -1s", "@every 0s", "CRON_TZ=UTC @every 500ms", "@every 1.5s"} {
		app := New(Config{Database: "app", BackupAll: "false", Schedule: spec}, nil, nil, nil)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if err := app.Run(ctx); err == nil || !strings.Contains(err.Error(), "invalid SCHEDULE") {
			t.Fatalf("schedule %q: %v", spec, err)
		}
	}
}

func TestInvalidSelectionFailsBeforeScheduling(t *testing.T) {
	for _, c := range []Config{
		{BackupAll: "false", Databases: "app,,billing"},
		{BackupAll: "false", Database: "app\nbilling"},
		{BackupAll: "true", Exclude: "scratch,"},
	} {
		c.Schedule = "@weekly"
		var log bytes.Buffer
		a := New(c, nil, &log, &log)
		ctx, cancel := context.WithCancel(t.Context())
		cancel() // A regression would otherwise wait for the scheduler.
		if err := a.Run(ctx); err == nil || !strings.Contains(err.Error(), "Database names") || log.Len() != 0 {
			t.Fatalf("invalid scheduled selection: err=%v log=%q", err, &log)
		}
	}
}

func TestUploadPartSizeConfiguration(t *testing.T) {
	for _, value := range []string{"", "5", "8", "64", "5120", "4", "5121", "-1", "1.5", "huge"} {
		t.Run(value, func(t *testing.T) {
			env := map[string]string{"S3_BUCKET": "bucket", "POSTGRES_USER": "user", "POSTGRES_PASSWORD": "secret", "POSTGRES_HOST": "db", "S3_UPLOAD_PART_SIZE_MB": value}
			c, err := loadConfig("backup", func(key string) (string, bool) { v, ok := env[key]; return v, ok })
			valid := value == "" || value == "5" || value == "8" || value == "64"
			if (err == nil) != valid {
				t.Fatalf("part size %q: %v", value, err)
			}
			if value == "64" && c.Storage.UploadPartSizeBytes != 64*1024*1024 {
				t.Fatalf("part size was not converted to bytes: %d", c.Storage.UploadPartSizeBytes)
			}
		})
	}
}

func TestRestoreOptionConfiguration(t *testing.T) {
	for _, name := range []string{"RESTORE_NO_OWNER", "RESTORE_NO_TABLESPACES", "RESTORE_NO_TABLE_ACCESS_METHOD"} {
		for _, value := range []string{"", "false", "true", "TRUE", "1", "no", " true"} {
			t.Run(name+"="+value, func(t *testing.T) {
				env := map[string]string{"S3_BUCKET": "bucket", "POSTGRES_USER": "user", "POSTGRES_PASSWORD": "secret", "POSTGRES_HOST": "db", name: value}
				c, err := loadConfig("restore", func(key string) (string, bool) { v, ok := env[key]; return v, ok })
				valid := value == "" || value == "false" || value == "true"
				if (err == nil) != valid || !valid && !strings.Contains(err.Error(), name) {
					t.Fatalf("%s=%q: error=%v", name, value, err)
				}
				if valid {
					for setting, enabled := range map[string]bool{
						"RESTORE_NO_OWNER":               c.RestoreNoOwner,
						"RESTORE_NO_TABLESPACES":         c.RestoreNoTablespaces,
						"RESTORE_NO_TABLE_ACCESS_METHOD": c.RestoreNoTableAccessMethod,
					} {
						if enabled != (setting == name && value == "true") {
							t.Errorf("%s=%q unexpectedly set %s to %v", name, value, setting, enabled)
						}
					}
				}
			})
		}
	}
}

func TestPartialBackupConfigurationAndDatabaseSelection(t *testing.T) {
	for _, value := range []string{"", "false", "true", "TRUE", "1", "no"} {
		env := map[string]string{"S3_BUCKET": "bucket", "POSTGRES_USER": "user", "POSTGRES_PASSWORD": "secret", "POSTGRES_HOST": "db", "BACKUP_ALLOW_PARTIAL": value}
		c, err := loadConfig("backup", func(key string) (string, bool) { v, ok := env[key]; return v, ok })
		valid := value == "" || value == "false" || value == "true"
		if (err == nil) != valid || valid && c.AllowPartial != (value == "true") {
			t.Fatalf("BACKUP_ALLOW_PARTIAL=%q: %#v, %v", value, c, err)
		}
	}
	for _, c := range []Config{
		{Database: "app", BackupAll: "false"}, {Databases: "app,billing", BackupAll: "false"},
		{BackupAll: "true", Exclude: "scratch"},
	} {
		if _, _, err := c.validateBackup(); err != nil {
			t.Errorf("database selection incorrectly requires partial permission: %v", err)
		}
		c.DumpOptions = "--table=orders"
		if _, _, err := c.validateBackup(); err == nil {
			t.Fatal("table selection accepted without partial permission")
		}
		c.AllowPartial = true
		if _, _, err := c.validateBackup(); err != nil {
			t.Errorf("explicit partial permission rejected: %v", err)
		}
	}
}

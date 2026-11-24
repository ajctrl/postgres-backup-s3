package backup

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/bartels/postgres-backup-s3/internal/storage"
)

// Two active parts, producer/read-ahead buffers and allocation headroom.
const uploadBufferParts = 6

type Config struct {
	Database, Databases, BackupAll, Exclude, MaintenanceDB string
	Host, Port, User, Password, DumpOptions                string
	Prefix, Passphrase, KeepDays, FilenameMode, Schedule   string
	RestoreNoOwner                                         bool
	RestoreNoTablespaces, RestoreNoTableAccessMethod       bool
	AllowPartial, AllowUnversionedOverwrite                bool
	DatabaseTimeout, RestoreTimeout                        time.Duration
	Storage                                                storage.Config
}

func LoadConfig(command string) (Config, error) {
	return loadConfig(command, os.LookupEnv)
}

func loadConfig(command string, lookup func(string) (string, bool)) (Config, error) {
	get := func(key string) string { value, _ := lookup(key); return value }
	valueOr := func(key, fallback string) string {
		if value := get(key); value != "" {
			return value
		}
		return fallback
	}
	prefix, present := lookup("S3_PREFIX")
	if !present {
		prefix = "backup"
	}
	c := Config{
		Database: get("POSTGRES_DATABASE"), Databases: get("POSTGRES_DATABASES"),
		BackupAll: valueOr("POSTGRES_BACKUP_ALL", "false"), Exclude: get("POSTGRES_DATABASES_EXCLUDE"),
		MaintenanceDB: valueOr("POSTGRES_MAINTENANCE_DB", "postgres"),
		Host:          get("POSTGRES_HOST"), Port: valueOr("POSTGRES_PORT", "5432"),
		User: get("POSTGRES_USER"), Password: get("POSTGRES_PASSWORD"), DumpOptions: get("PGDUMP_EXTRA_OPTS"),
		Prefix: prefix, Passphrase: get("PASSPHRASE"), KeepDays: get("BACKUP_KEEP_DAYS"),
		FilenameMode: valueOr("BACKUP_FILENAME_MODE", "timestamp"), Schedule: get("SCHEDULE"),
		Storage: storage.Config{
			Bucket: get("S3_BUCKET"), Region: valueOr("S3_REGION", "us-west-1"), Endpoint: get("S3_ENDPOINT"),
			AccessKeyID: get("S3_ACCESS_KEY_ID"), SecretAccessKey: get("S3_SECRET_ACCESS_KEY"), SessionToken: get("S3_SESSION_TOKEN"),
		},
	}
	for _, required := range []struct{ name, value string }{
		{"S3_BUCKET", c.Storage.Bucket}, {"POSTGRES_USER", c.User}, {"POSTGRES_PASSWORD", c.Password},
	} {
		if required.value == "" {
			return c, fmt.Errorf("You need to set %s.", required.name)
		}
	}
	if c.Host == "" {
		c.Host = get("POSTGRES_PORT_5432_TCP_ADDR")
		if c.Host == "" {
			return c, fmt.Errorf("You need to set POSTGRES_HOST.")
		}
		c.Port = valueOr("POSTGRES_PORT_5432_TCP_PORT", "5432")
	}
	if c.FilenameMode != "timestamp" && c.FilenameMode != "fixed" {
		return c, fmt.Errorf("BACKUP_FILENAME_MODE must be timestamp or fixed.")
	}
	var err error
	boolSetting := func(name string, dst *bool) error {
		value, ok := lookup(name)
		if !ok {
			return nil
		}
		if value == "" && name != "BACKUP_ALLOW_UNVERSIONED_OVERWRITE" {
			value = "false"
		}
		switch value {
		case "true":
			*dst = true
		case "false":
			*dst = false
		default:
			return fmt.Errorf("%s must be true or false.", name)
		}
		return nil
	}
	if command == "restore" {
		if value := get("RESTORE_TIMEOUT"); value != "" {
			c.RestoreTimeout, err = time.ParseDuration(value)
			if err != nil || c.RestoreTimeout <= 0 {
				return c, fmt.Errorf("RESTORE_TIMEOUT must be a positive duration")
			}
		}
		for _, setting := range []struct {
			name string
			dst  *bool
		}{
			{"RESTORE_NO_OWNER", &c.RestoreNoOwner}, {"RESTORE_NO_TABLESPACES", &c.RestoreNoTablespaces}, {"RESTORE_NO_TABLE_ACCESS_METHOD", &c.RestoreNoTableAccessMethod},
		} {
			if err := boolSetting(setting.name, setting.dst); err != nil {
				return c, err
			}
		}
	} else {
		if err := boolSetting("BACKUP_ALLOW_PARTIAL", &c.AllowPartial); err != nil {
			return c, err
		}
		if err := boolSetting("BACKUP_ALLOW_UNVERSIONED_OVERWRITE", &c.AllowUnversionedOverwrite); err != nil {
			return c, err
		}
		partSize, err := strconv.ParseInt(valueOr("S3_UPLOAD_PART_SIZE_MB", "8"), 10, 64)
		if err != nil || partSize < 5 || partSize > 5120 {
			return c, fmt.Errorf("S3_UPLOAD_PART_SIZE_MB must be an integer between 5 and 5120 (MiB).")
		}
		budget, err := strconv.ParseInt(valueOr("S3_UPLOAD_BUFFER_BUDGET_MB", "1024"), 10, 64)
		if err != nil || budget <= 0 || partSize*uploadBufferParts > budget {
			return c, fmt.Errorf("S3_UPLOAD_BUFFER_BUDGET_MB must cover at least %d MiB for %d MiB parts", partSize*uploadBufferParts, partSize)
		}
		c.Storage.UploadPartSizeBytes = partSize * 1024 * 1024
		if value := get("BACKUP_DATABASE_TIMEOUT"); value != "" {
			c.DatabaseTimeout, err = time.ParseDuration(value)
			if err != nil || c.DatabaseTimeout <= 0 {
				return c, fmt.Errorf("BACKUP_DATABASE_TIMEOUT must be a positive duration such as 30m or 24h.")
			}
		}
	}
	if err := validatePassphrase(c.Passphrase); err != nil {
		return c, err
	}
	return c, nil
}

func (c Config) validateBackup() (int, *dumpOptions, error) {
	days, err := c.validateSelectionAndRetention()
	if err != nil {
		return 0, nil, err
	}
	opts, err := parseDumpOptions(c.DumpOptions, c.AllowPartial)
	return days, opts, err
}

func (c Config) validateSelectionAndRetention() (int, error) {
	if err := validatePassphrase(c.Passphrase); err != nil {
		return 0, err
	}
	switch c.BackupAll {
	case "true":
		if c.Database != "" || c.Databases != "" {
			return 0, fmt.Errorf("POSTGRES_BACKUP_ALL cannot be combined with POSTGRES_DATABASE or POSTGRES_DATABASES.")
		}
	case "false":
		if c.Database == "" && c.Databases == "" {
			return 0, fmt.Errorf("Set POSTGRES_DATABASE, POSTGRES_DATABASES, or POSTGRES_BACKUP_ALL=true.")
		}
		if err := validateDatabases(c.explicitDatabases()); err != nil {
			return 0, err
		}
	default:
		return 0, fmt.Errorf("POSTGRES_BACKUP_ALL must be true or false.")
	}
	if c.Exclude != "" {
		if c.BackupAll != "true" {
			return 0, fmt.Errorf("POSTGRES_DATABASES_EXCLUDE requires POSTGRES_BACKUP_ALL=true.")
		}
		if err := validateDatabases(parseDatabaseList(c.Exclude)); err != nil {
			return 0, err
		}
	}
	if c.KeepDays == "" {
		return 0, nil
	}
	if c.KeepDays[0] == '0' || strings.IndexFunc(c.KeepDays, func(r rune) bool { return r < '0' || r > '9' }) >= 0 {
		return 0, fmt.Errorf("BACKUP_KEEP_DAYS must be a positive integer without leading zeros.")
	}
	days, err := strconv.Atoi(c.KeepDays)
	if err != nil || days < 1 || days > 36500 {
		return 0, fmt.Errorf("BACKUP_KEEP_DAYS must be at most 36500.")
	}
	return days, nil
}

func (c Config) explicitDatabases() []string {
	if c.Databases != "" {
		return parseDatabaseList(c.Databases)
	}
	return []string{c.Database}
}

func parseDatabaseList(value string) []string {
	names := strings.Split(value, ",")
	for i := range names {
		names[i] = strings.TrimSpace(names[i])
	}
	return names
}

func validateDatabases(names []string) error {
	if len(names) == 0 {
		return fmt.Errorf("Database names must be nonempty and contain no control characters.")
	}
	for _, name := range names {
		if name == "" || !utf8.ValidString(name) || strings.IndexFunc(name, func(r rune) bool { return r < 32 || r == 127 }) >= 0 {
			return fmt.Errorf("Database names must be nonempty and contain no control characters.")
		}
	}
	return nil
}

// Match jq's @uri encoding, including slashes, percent signs and UTF-8 bytes.
// PathEscape leaves several reserved characters unescaped; QueryEscape uses '+'.
func encodeComponent(value string) string {
	const hex = "0123456789ABCDEF"
	var result strings.Builder
	for i := 0; i < len(value); i++ {
		b := value[i]
		if b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || strings.ContainsRune("-._~", rune(b)) {
			result.WriteByte(b)
		} else {
			result.WriteByte('%')
			result.WriteByte(hex[b>>4])
			result.WriteByte(hex[b&15])
		}
	}
	return result.String()
}

func databaseURI(database string) string { return "postgresql:///" + encodeComponent(database) }

func (c Config) fileType() string {
	if c.Passphrase != "" {
		return ".dump.gpg"
	}
	return ".dump"
}

func (c Config) databaseKeyBase(database string) string {
	prefix := strings.TrimRight(c.Prefix, "/")
	if prefix != "" {
		prefix += "/"
	}
	scope := "v2/full/"
	if c.AllowPartial {
		scope = "v2/partial/"
	}
	return prefix + scope + databaseKeyComponent(database)
}

func (c Config) timestampPrefix(database string) string {
	return c.databaseKeyBase(database) + "/"
}

func (c Config) fixedKey(database string) string {
	return c.databaseKeyBase(database) + c.fileType()
}

func databaseKeyComponent(database string) string {
	component := encodeComponent(database)
	if component == "." || component == ".." {
		component = strings.ReplaceAll(component, ".", "%2E")
	}
	return component
}

func (c Config) backupKey(database string, now time.Time) (string, error) {
	if c.FilenameMode == "fixed" {
		return c.fixedKey(database), nil
	}
	generation, err := newGeneration(now)
	if err != nil {
		return "", err
	}
	return c.timestampPrefix(database) + generation + c.fileType(), nil
}

func (c Config) isTimestampBackup(database, key string) bool {
	prefix := c.timestampPrefix(database)
	if !strings.HasPrefix(key, prefix) {
		return false
	}
	name := strings.TrimSuffix(strings.TrimPrefix(key, prefix), ".gpg")
	return strings.HasSuffix(name, ".dump") && validGeneration(strings.TrimSuffix(name, ".dump"))
}

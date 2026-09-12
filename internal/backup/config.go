package backup

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/bartels/postgres-backup-s3/internal/storage"
)

const timestampLayout = "2006-01-02T15:04:05"

var timestampPattern = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}$`)
var backupSuffixPattern = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.dump(\.gpg)?$`)

type Config struct {
	Database, Databases, BackupAll, Exclude, MaintenanceDB string
	Host, Port, User, Password, DumpOptions                string
	Prefix, Passphrase, KeepDays, FilenameMode, Schedule   string
	Storage                                                storage.Config
}

func LoadConfig() (Config, error) {
	return loadConfig(os.LookupEnv)
}

func loadConfig(lookup func(string) (string, bool)) (Config, error) {
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
			AccessKeyID: get("S3_ACCESS_KEY_ID"), SecretAccessKey: get("S3_SECRET_ACCESS_KEY"),
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
	return c, nil
}

func (c Config) validateBackup() (int, error) {
	switch c.BackupAll {
	case "true":
		if c.Database != "" || c.Databases != "" {
			return 0, fmt.Errorf("POSTGRES_BACKUP_ALL cannot be combined with POSTGRES_DATABASE or POSTGRES_DATABASES.")
		}
	case "false":
		if c.Database == "" && c.Databases == "" {
			return 0, fmt.Errorf("Set POSTGRES_DATABASE, POSTGRES_DATABASES, or POSTGRES_BACKUP_ALL=true.")
		}
	default:
		return 0, fmt.Errorf("POSTGRES_BACKUP_ALL must be true or false.")
	}
	if c.Exclude != "" && c.BackupAll != "true" {
		return 0, fmt.Errorf("POSTGRES_DATABASES_EXCLUDE requires POSTGRES_BACKUP_ALL=true.")
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

func (c Config) timestampPrefix(database string) string { return c.Prefix + "/" + database + "_" }

func (c Config) fixedKey(database string) string {
	prefix := strings.TrimSuffix(c.Prefix, "/")
	if prefix != "" {
		prefix += "/"
	}
	directory := encodeComponent(database)
	if directory == "." || directory == ".." {
		directory = strings.ReplaceAll(directory, ".", "%2E")
	}
	return prefix + directory + "/latest" + c.fileType()
}

func (c Config) backupKey(database string, now time.Time) string {
	if c.FilenameMode == "fixed" {
		return c.fixedKey(database)
	}
	return c.timestampPrefix(database) + now.UTC().Format(timestampLayout) + c.fileType()
}

func (c Config) isTimestampBackup(database, key string) bool {
	prefix := c.timestampPrefix(database)
	return strings.HasPrefix(key, prefix) && backupSuffixPattern.MatchString(strings.TrimPrefix(key, prefix))
}

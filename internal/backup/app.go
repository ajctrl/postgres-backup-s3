package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/bartels/postgres-backup-s3/internal/storage"
	"github.com/robfig/cron/v3"
)

type App struct {
	Config   Config
	Store    storage.Store
	Out, Err io.Writer
	now      func() time.Time
	lockPath string
}

func New(c Config, store storage.Store, out, stderr io.Writer) *App {
	return &App{Config: c, Store: store, Out: out, Err: stderr, now: time.Now, lockPath: "/tmp/postgres-backup-s3.lock"}
}

func scheduleParser() cron.Parser {
	return cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
}

func (a *App) Run(ctx context.Context) error {
	if a.Config.Schedule == "" {
		return a.Backup(ctx)
	}
	if _, err := a.Config.validateBackup(); err != nil {
		return err
	}
	schedule, err := scheduleParser().Parse(a.Config.Schedule)
	if err != nil {
		return fmt.Errorf("invalid SCHEDULE: %w", err)
	}
	if schedule.Next(a.now()).IsZero() {
		return fmt.Errorf("SCHEDULE has no future execution time")
	}
	scheduler := cron.New()
	scheduler.Schedule(schedule, cron.FuncJob(func() {
		if err := a.Backup(ctx); err != nil {
			fmt.Fprintf(a.Err, "Scheduled backup failed: %v\n", err)
		}
	}))
	fmt.Fprintf(a.Out, "Scheduling backups: %s\n", a.Config.Schedule)
	scheduler.Start()
	<-ctx.Done()
	// Cancellation reaches SDK requests and subprocess groups. Wait for cleanup
	// and lock release before PID 1 exits.
	<-scheduler.Stop().Done()
	return ctx.Err()
}

func (a *App) runner(lock *os.File) commandRunner {
	return commandRunner{password: a.Config.Password, stderr: a.Err, lock: lock}
}

func withWorkspace(action func(string) error) (err error) {
	dir, err := os.MkdirTemp("", "postgres-backup-s3-")
	if err != nil {
		return err
	}
	defer func() {
		if cleanupErr := os.RemoveAll(dir); cleanupErr != nil {
			err = errors.Join(err, fmt.Errorf("remove temporary backup files: %w", cleanupErr))
		}
	}()
	return action(dir)
}

func (a *App) Backup(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	lock, err := acquireLock(a.lockPath)
	if err != nil {
		return err
	}
	defer lock.Close()
	days, err := a.Config.validateBackup()
	if err != nil {
		return err
	}
	// The shell implementation captured epoch seconds at run start. Preserve
	// that boundary so objects at the cutoff second are still retained.
	cutoff := a.now().UTC().Truncate(time.Second).Add(-time.Duration(days) * 24 * time.Hour)
	runner := a.runner(lock)
	if a.Config.FilenameMode == "fixed" {
		enabled, err := a.Store.VersioningEnabled(ctx)
		if err != nil {
			return fmt.Errorf("Could not verify S3 bucket versioning: %w", err)
		}
		if !enabled {
			return fmt.Errorf("Fixed filenames require S3 bucket versioning to be Enabled.")
		}
	}
	databases, err := a.databases(ctx, runner)
	if err != nil {
		return err
	}
	succeeded, failed, cleanupFailed := 0, 0, 0
	for _, database := range databases {
		if ctx.Err() != nil {
			break
		}
		if err := a.backupDatabase(ctx, runner, database); err != nil {
			fmt.Fprintf(a.Err, "Backup failed: %s: %v\n", database, err)
			failed++
		} else {
			succeeded++
			if days > 0 {
				if err := a.removeOldBackups(ctx, database, cutoff); err != nil {
					fmt.Fprintf(a.Err, "Retention cleanup failed: %s: %v\n", database, err)
					cleanupFailed++
				}
			}
		}
	}
	fmt.Fprintf(a.Out, "Backup summary: %d succeeded, %d failed, %d retention cleanups failed.\n", succeeded, failed, cleanupFailed)
	if err := ctx.Err(); err != nil {
		return err
	}
	if failed > 0 || cleanupFailed > 0 {
		return fmt.Errorf("backup run completed with failures")
	}
	return nil
}

func (a *App) connectionArgs(database string) []string {
	return []string{"-d", databaseURI(database), "-h", a.Config.Host, "-p", a.Config.Port, "-U", a.Config.User}
}

func (a *App) databases(ctx context.Context, runner commandRunner) ([]string, error) {
	c := a.Config
	var names []string
	if c.BackupAll == "true" {
		fmt.Fprintln(a.Out, "Discovering databases...")
		args := append([]string{"-X", "-A", "-t", "-v", "ON_ERROR_STOP=1"}, a.connectionArgs(c.MaintenanceDB)...)
		args = append(args, "-c", "SELECT COALESCE(json_agg(datname ORDER BY datname), '[]'::json) FROM pg_database WHERE NOT datistemplate AND datallowconn;")
		var output bytes.Buffer
		if err := runner.run(ctx, &output, "psql", args...); err != nil {
			return nil, fmt.Errorf("Could not discover databases: %w", err)
		}
		if err := json.Unmarshal(output.Bytes(), &names); err != nil {
			return nil, fmt.Errorf("invalid database discovery result: %w", err)
		}
	} else if c.Databases != "" {
		names = parseDatabaseList(c.Databases)
	} else {
		names = []string{c.Database}
	}
	if err := validateDatabases(names); err != nil {
		return nil, err
	}
	excluded := make(map[string]bool)
	if c.Exclude != "" {
		list := parseDatabaseList(c.Exclude)
		if err := validateDatabases(list); err != nil {
			return nil, err
		}
		for _, name := range list {
			excluded[name] = true
		}
	}
	seen := make(map[string]bool)
	var selected []string
	for _, name := range names {
		if excluded[name] {
			fmt.Fprintf(a.Out, "Excluding database: %s\n", name)
		} else if !seen[name] {
			selected = append(selected, name)
			seen[name] = true
		}
	}
	if len(selected) == 0 {
		return nil, fmt.Errorf("No databases selected for backup.")
	}
	return selected, nil
}

func (a *App) backupDatabase(ctx context.Context, runner commandRunner, database string) error {
	key := a.Config.backupKey(database, a.now())
	fmt.Fprintf(a.Out, "Creating backup of %s database...\n", database)
	args := append([]string{"--format=custom"}, a.connectionArgs(database)...)
	// Preserve shell IFS splitting; quotes and wildcards remain literal data.
	opts := strings.FieldsFunc(a.Config.DumpOptions, func(r rune) bool { return r == ' ' || r == '\t' || r == '\n' })
	commands := []pipelineCommand{{name: "pg_dump", args: append(args, opts...)}}
	if a.Config.Passphrase != "" {
		fmt.Fprintf(a.Out, "Encrypting backup of %s...\n", database)
		commands = append(commands, pipelineCommand{name: "gpg", args: []string{
			"--symmetric", "--batch", "--pinentry-mode", "loopback", "--passphrase", a.Config.Passphrase, "--output", "-",
		}})
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	reader, writer := io.Pipe()
	defer reader.Close()
	// Closing the reader also unblocks os/exec's stdout copier on cancellation.
	stop := context.AfterFunc(ctx, func() { reader.CloseWithError(ctx.Err()) })
	defer stop()
	done := make(chan error, 1)
	go func() {
		err := runner.pipeline(ctx, writer, commands...)
		writer.CloseWithError(err)
		done <- err
	}()
	fmt.Fprintf(a.Out, "Uploading backup of %s...\n", database)
	uploadErr := a.Store.Upload(ctx, key, reader)
	// An upload failure must stop both pg_dump and GPG and release backpressure.
	if uploadErr != nil {
		cancel()
		reader.CloseWithError(uploadErr)
	}
	if err := errors.Join(uploadErr, <-done); err != nil {
		return err
	}
	fmt.Fprintf(a.Out, "Backup complete: %s\n", database)
	return nil
}

func (a *App) timestampBackups(ctx context.Context, database string) ([]storage.Object, error) {
	objects, err := a.Store.List(ctx, a.Config.timestampPrefix(database))
	if err != nil {
		return nil, err
	}
	var backups []storage.Object
	for _, object := range objects {
		if a.Config.isTimestampBackup(database, object.Key) {
			backups = append(backups, object)
		}
	}
	return backups, nil
}

func (a *App) removeOldBackups(ctx context.Context, database string, cutoff time.Time) error {
	objects, err := a.timestampBackups(ctx, database)
	if err != nil {
		return err
	}
	for _, object := range objects {
		// Missing timestamps must never turn an incomplete response into deletion.
		if object.LastModified.IsZero() {
			return fmt.Errorf("missing LastModified for %q", object.Key)
		}
	}
	for _, object := range objects {
		if err := ctx.Err(); err != nil {
			return err
		}
		if object.LastModified.Before(cutoff) {
			if err := a.Store.Delete(ctx, object.Key); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *App) Restore(ctx context.Context, args []string) error {
	c := a.Config
	if c.Database == "" {
		return fmt.Errorf("Set POSTGRES_DATABASE to the single database to restore.")
	}
	if err := validateDatabases([]string{c.Database}); err != nil {
		return err
	}
	timestamp, versionID := "", ""
	switch len(args) {
	case 0:
	case 1:
		if c.FilenameMode != "timestamp" {
			return fmt.Errorf("Use --version-id ID to restore a previous fixed-name backup.")
		}
		if !timestampPattern.MatchString(args[0]) {
			return fmt.Errorf("Invalid backup timestamp.")
		}
		timestamp = args[0]
	case 2:
		if args[0] != "--version-id" || args[1] == "" || c.FilenameMode != "fixed" {
			return fmt.Errorf("--version-id ID requires BACKUP_FILENAME_MODE=fixed.")
		}
		versionID = args[1]
	default:
		return fmt.Errorf("Usage: restore.sh [timestamp | --version-id ID]")
	}
	return withWorkspace(func(dir string) error {
		var key string
		switch {
		case c.FilenameMode == "fixed":
			key = c.fixedKey(c.Database)
		case timestamp != "":
			key = c.timestampPrefix(c.Database) + timestamp + c.fileType()
		default:
			fmt.Fprintf(a.Out, "Finding latest backup of %s...\n", c.Database)
			objects, err := a.timestampBackups(ctx, c.Database)
			if err != nil {
				return fmt.Errorf("Could not list backups: %w", err)
			}
			for _, object := range objects {
				if strings.HasSuffix(object.Key, c.fileType()) && object.Key > key {
					key = object.Key
				}
			}
			if key == "" {
				return fmt.Errorf("No backup found for %s.", c.Database)
			}
		}
		fmt.Fprintf(a.Out, "Fetching backup of %s from S3...\n", c.Database)
		if err := a.Store.Download(ctx, key, versionID, filepath.Join(dir, "db"+c.fileType())); err != nil {
			return err
		}
		runner := a.runner(nil)
		if c.Passphrase != "" {
			fmt.Fprintln(a.Out, "Decrypting backup...")
			file, err := os.OpenFile(filepath.Join(dir, "db.dump"), os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)
			if err != nil {
				return err
			}
			err = runner.run(ctx, file, "gpg", "--decrypt", "--batch", "--pinentry-mode", "loopback", "--passphrase", c.Passphrase, filepath.Join(dir, "db.dump.gpg"))
			err = errors.Join(err, file.Close())
			if err != nil {
				return err
			}
		}
		fmt.Fprintf(a.Out, "Restoring %s from backup...\n", c.Database)
		args := append(a.connectionArgs(c.Database), "--exit-on-error", "--clean", "--if-exists", filepath.Join(dir, "db.dump"))
		if err := runner.run(ctx, a.Out, "pg_restore", args...); err != nil {
			return err
		}
		fmt.Fprintln(a.Out, "Restore complete.")
		return nil
	})
}

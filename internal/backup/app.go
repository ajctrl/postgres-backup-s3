package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/aws/smithy-go"
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
	started := a.now().Round(0)
	partSize := a.Config.Storage.UploadPartSizeBytes
	if partSize == 0 {
		partSize = storage.DefaultUploadPartSizeBytes
	}
	maxBytes := partSize * storage.MaxUploadParts
	fmt.Fprintf(a.Out, "S3 upload settings: part size %d MiB; multipart limit %d parts, %d bytes (%.3f GiB) per database backup after compression and any encryption.\n",
		partSize/(1024*1024), storage.MaxUploadParts, maxBytes, float64(maxBytes)/(1024*1024*1024))
	runner := a.runner(lock)
	opts, err := parseDumpOptions(a.Config.DumpOptions, a.Config.AllowPartial)
	if err != nil {
		return err
	}
	if err := runner.checkDumpClient(ctx, opts.minClientMajor); err != nil {
		return err
	}
	if opts.partial {
		fmt.Fprintln(a.Err, "WARNING: partial database backup explicitly enabled; selected contents may omit data, definitions, or dependencies needed for restore.")
	}
	if a.Config.FilenameMode == "fixed" {
		enabled, err := a.Store.VersioningEnabled(ctx)
		if err != nil {
			var apiErr smithy.APIError
			if !errors.As(err, &apiErr) || apiErr.ErrorCode() != "AccessDenied" {
				return fmt.Errorf("Could not verify S3 bucket versioning: %w", err)
			}
			fmt.Fprintf(a.Err, "Warning: Could not verify S3 bucket versioning: %v. Continuing backup; ensure bucket versioning is Enabled to retain previous fixed-name backups.\n", err)
		} else if !enabled {
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
		databaseStarted := a.now().Round(0)
		if databaseStarted.Before(started) {
			return errClockRollback
		}
		checkClock := func() error { return a.checkClock(databaseStarted) }
		key, err := a.backupDatabase(ctx, runner, database, databaseStarted)
		if err != nil {
			fmt.Fprintf(a.Err, "Backup failed: %s: %v\n", database, err)
			if errors.Is(err, errClockRollback) {
				return err
			}
			failed++
		} else {
			succeeded++
			if days > 0 {
				if err := a.removeOldBackups(ctx, database, key, days, checkClock); err != nil {
					fmt.Fprintf(a.Err, "Retention cleanup failed: %s: %v\n", database, err)
					if errors.Is(err, errClockRollback) {
						return err
					}
					cleanupFailed++
				}
			}
		}
		if err := checkClock(); err != nil {
			return err
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
		args = append(args, "-c", "SELECT COALESCE(pg_catalog.json_agg(datname ORDER BY datname), '[]'::pg_catalog.json) FROM pg_catalog.pg_database WHERE NOT datistemplate AND datallowconn;")
		if err := runner.queryJSON(ctx, &names, args...); err != nil {
			return nil, fmt.Errorf("Could not discover databases: %w", err)
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

var errClockRollback = errors.New("wall clock moved backwards; backup stopped")

func (a *App) checkClock(started time.Time) error {
	// Remove Go's monotonic component: this check is about wall-clock changes.
	if ended := a.now().Round(0); ended.Before(started.Round(0)) {
		return fmt.Errorf("%w: start=%s current=%s", errClockRollback, started.Format(time.RFC3339Nano), ended.Format(time.RFC3339Nano))
	}
	return nil
}

func (a *App) backupDatabase(ctx context.Context, runner commandRunner, database string, started time.Time) (string, error) {
	key, err := a.Config.backupKey(database, started)
	if err != nil {
		return "", err
	}
	standby, err := a.checkBackupSource(ctx, runner, database)
	if err != nil {
		return "", err
	}
	fmt.Fprintf(a.Out, "Creating backup of %s database...\n", database)
	args := append([]string{"--format=custom"}, a.dumpConnectionArgs(database, standby)...)
	opts, err := parseDumpOptions(a.Config.DumpOptions, a.Config.AllowPartial)
	if err != nil {
		return "", err
	}
	commands := []pipelineCommand{{name: "pg_dump", args: append(args, opts.args...), customDump: true, rejectUnlogged: standby && !a.Config.AllowPartial}}
	if a.Config.Passphrase != "" {
		fmt.Fprintf(a.Out, "Encrypting backup of %s...\n", database)
		commands = append(commands, pipelineCommand{name: "gpg", passphrase: a.Config.Passphrase, args: []string{
			"--symmetric", "--batch", "--pinentry-mode", "loopback", "--output", "-",
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
	uploadErr := a.Store.Upload(ctx, key, reader, storage.UploadOptions{
		IfAbsent:      a.Config.FilenameMode != "fixed",
		BeforePublish: func() error { return a.checkClock(started) },
	})
	// An upload failure must stop both pg_dump and GPG and release backpressure.
	if uploadErr != nil {
		cancel()
		reader.CloseWithError(uploadErr)
	}
	if err := errors.Join(uploadErr, <-done); err != nil {
		return "", err
	}
	if err := a.checkClock(started); err != nil {
		return "", err
	}
	fmt.Fprintf(a.Out, "Backup complete: %s; key: %s\n", database, key)
	return key, nil
}

func (a *App) timestampBackups(ctx context.Context, database string) (storage.Listing, error) {
	listing, err := a.Store.List(ctx, a.Config.timestampPrefix(database))
	if err != nil {
		return storage.Listing{}, err
	}
	var backups []storage.Object
	for _, object := range listing.Objects {
		if a.Config.isTimestampBackup(database, object.Key) {
			backups = append(backups, object)
		}
	}
	listing.Objects = backups
	return listing, nil
}

func (a *App) removeOldBackups(ctx context.Context, database, publishedKey string, days int, checkClock func() error) error {
	if err := checkClock(); err != nil {
		return err
	}
	listing, err := a.timestampBackups(ctx, database)
	if err != nil {
		return err
	}
	if listing.ServerTime.IsZero() {
		return fmt.Errorf("S3 listing has no valid server Date; backup retained, retention stopped")
	}
	cutoff := listing.ServerTime.UTC().Add(-time.Duration(days) * 24 * time.Hour)
	for _, object := range listing.Objects {
		// Missing timestamps must never turn an incomplete response into deletion.
		if object.LastModified.IsZero() {
			return fmt.Errorf("missing LastModified for %q", object.Key)
		}
	}
	for _, object := range listing.Objects {
		if err := ctx.Err(); err != nil {
			return err
		}
		if object.Key != publishedKey && object.LastModified.Before(cutoff) {
			if err := checkClock(); err != nil {
				return err
			}
			if err := a.Store.Delete(ctx, object.Key, checkClock); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *App) Restore(ctx context.Context, args []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c := a.Config
	if c.Database == "" {
		return fmt.Errorf("Set POSTGRES_DATABASE to the single database to restore.")
	}
	if err := validateDatabases([]string{c.Database}); err != nil {
		return err
	}
	if err := validatePassphrase(c.Passphrase); err != nil {
		return err
	}
	generation, versionID := "", ""
	switch len(args) {
	case 0:
	case 1:
		if c.FilenameMode != "timestamp" {
			return fmt.Errorf("Use --version-id ID to restore a previous fixed-name backup.")
		}
		if !validGeneration(args[0]) {
			return fmt.Errorf("Invalid backup generation; use the full timestamp-ID name without .dump or .dump.gpg.")
		}
		generation = args[0]
	case 2:
		if args[0] != "--version-id" || args[1] == "" || c.FilenameMode != "fixed" {
			return fmt.Errorf("--version-id ID requires BACKUP_FILENAME_MODE=fixed.")
		}
		versionID = args[1]
	default:
		return fmt.Errorf("Usage: restore.sh [generation | --version-id ID]")
	}
	lock, err := acquireLock(a.lockPath)
	if err != nil {
		return err
	}
	defer lock.Close()
	return withWorkspace(func(dir string) error {
		var key string
		switch {
		case c.FilenameMode == "fixed":
			key = c.fixedKey(c.Database)
		case generation != "":
			key = c.timestampPrefix(c.Database) + generation + c.fileType()
		default:
			fmt.Fprintf(a.Out, "Finding latest backup of %s...\n", c.Database)
			objects, err := a.timestampBackups(ctx, c.Database)
			if err != nil {
				return fmt.Errorf("Could not list backups: %w", err)
			}
			var latest time.Time
			for _, object := range objects.Objects {
				if strings.HasSuffix(object.Key, c.fileType()) && (key == "" || object.LastModified.After(latest) || (object.LastModified.Equal(latest) && object.Key > key)) {
					key = object.Key
					latest = object.LastModified
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
		runner := a.runner(lock)
		if c.Passphrase != "" {
			fmt.Fprintln(a.Out, "Decrypting backup...")
			file, err := os.OpenFile(filepath.Join(dir, "db.dump"), os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)
			if err != nil {
				return err
			}
			err = runner.pipeline(ctx, file, pipelineCommand{name: "gpg", passphrase: c.Passphrase, args: []string{
				"--decrypt", "--batch", "--pinentry-mode", "loopback", filepath.Join(dir, "db.dump.gpg"),
			}})
			err = errors.Join(err, file.Close())
			if err != nil {
				return err
			}
		}
		path := filepath.Join(dir, "db.dump")
		if err := validateRestoreArchive(ctx, path); err != nil {
			return err
		}
		fmt.Fprintf(a.Out, "Restoring %s from backup...\n", c.Database)
		args := append(a.connectionArgs(c.Database), "--single-transaction", "--exit-on-error", "--clean", "--if-exists")
		if c.RestoreNoOwner {
			args = append(args, "--no-owner")
		}
		if c.RestoreNoTablespaces {
			args = append(args, "--no-tablespaces")
		}
		if c.RestoreNoTableAccessMethod {
			args = append(args, "--no-table-access-method")
		}
		args = append(args, path)
		if err := runner.restore(ctx, args...); err != nil {
			return err
		}
		fmt.Fprintln(a.Out, "Restore complete.")
		return nil
	})
}

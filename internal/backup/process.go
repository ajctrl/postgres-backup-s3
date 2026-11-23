package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"
)

type commandRunner struct {
	password string
	stderr   io.Writer
	lock     *os.File
}

// Metadata uses only trusted schema objects and UTF-8, independently of role,
// service-file and environment defaults. SET runs after connecting so it also
// overrides database/role settings without discarding unrelated libpq options.
func (r commandRunner) queryJSON(ctx context.Context, value any, args ...string) error {
	// Quiet suppresses SET's command tag; stdout must contain only query JSON.
	// Explicit pg_temp placement keeps temporary relations behind pg_catalog.
	args = append([]string{"--quiet", "-c", "SET search_path = pg_catalog, pg_temp"}, args...)
	cmd := r.command(ctx, "psql", args...)
	cmd.Env = append(withoutEnv(cmd.Env, "PGCLIENTENCODING"), "PGCLIENTENCODING=UTF8")
	var output bytes.Buffer
	cmd.Stdout = &output
	if err := commandError(ctx, "psql", cmd.Run()); err != nil {
		return err
	}
	if !utf8.Valid(output.Bytes()) {
		return fmt.Errorf("invalid UTF-8 in PostgreSQL metadata response")
	}
	if err := json.Unmarshal(output.Bytes(), value); err != nil {
		// Do not echo raw metadata or SQL in errors.
		return fmt.Errorf("invalid PostgreSQL metadata JSON")
	}
	return nil
}

func (r commandRunner) restore(ctx context.Context, args ...string) error {
	cmd := r.command(ctx, "pg_restore", args...)
	// Both SQL and server diagnostics can contain credentials and row values.
	// Discard at the source, rather than attempting language-dependent redaction.
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	if err := commandError(ctx, "pg_restore", cmd.Run()); err != nil {
		return fmt.Errorf("%w (SQL and server diagnostics suppressed to protect database secrets)", err)
	}
	return nil
}

func (r commandRunner) run(ctx context.Context, stdout io.Writer, name string, args ...string) error {
	cmd := r.command(ctx, name, args...)
	cmd.Stdout = stdout
	return commandError(ctx, name, cmd.Run())
}

// Probe the executable actually used by the pipeline, rather than trusting an
// environment variable describing the image. Only version-specific options
// require this extra subprocess, and it runs before discovery or S3 access.
func (r commandRunner) checkDumpClient(ctx context.Context, minimum int) error {
	if minimum == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var output strings.Builder
	if err := r.run(ctx, &output, "pg_dump", "--version"); err != nil {
		return fmt.Errorf("PGDUMP_EXTRA_OPTS: cannot verify pg_dump version: %w", err)
	}
	fields := strings.Fields(output.String())
	if len(fields) < 3 || fields[0] != "pg_dump" || fields[1] != "(PostgreSQL)" {
		return fmt.Errorf("PGDUMP_EXTRA_OPTS: cannot recognize pg_dump version")
	}
	major, err := strconv.Atoi(strings.SplitN(fields[2], ".", 2)[0])
	if err != nil || major < minimum {
		return fmt.Errorf("PGDUMP_EXTRA_OPTS: --no-statistics requires pg_dump %d or newer; configured client is incompatible or unrecognized", minimum)
	}
	return nil
}

func (r commandRunner) command(ctx context.Context, name string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stderr = r.stderr
	cmd.Env = append(withoutEnv(withoutEnv(os.Environ(), "PGPASSWORD"), "PASSPHRASE"), "PGPASSWORD="+r.password)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if r.lock != nil {
		// The child keeps the operation lock if the parent is killed abruptly.
		cmd.ExtraFiles = []*os.File{r.lock}
	}
	cmd.Cancel = func() error {
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	cmd.WaitDelay = 5 * time.Second
	return cmd
}

func commandError(ctx context.Context, name string, err error) error {
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Never include argv here: command arguments may contain sensitive data.
		return fmt.Errorf("%s failed: %w", name, err)
	}
	return nil
}

type pipelineCommand struct {
	name                       string
	args                       []string
	passphrase                 string
	customDump                 bool
	rejectUnlogged             bool
	rejectMissingSubscriptions bool
}

type synchronizedWriter struct {
	mu  *sync.Mutex
	dst io.Writer
}

func (w *synchronizedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.dst.Write(p)
}

// pg_dump 17 and 18 report this omission only on stderr, after applying
// --role on the actual dump connection. Wait drains stderr before the
// pipeline can signal a successful EOF to the uploader.
const subscriptionWarning = "\npg_dump: warning: subscriptions not dumped because current user is not a superuser\n"

type subscriptionGuard struct {
	tail   []byte
	cancel context.CancelCauseFunc
}

func (g *subscriptionGuard) Write(p []byte) (int, error) {
	data := append(g.tail, p...)
	if bytes.Contains(data, []byte(subscriptionWarning)) {
		g.cancel(errors.New("pg_dump omitted subscriptions; use a superuser dump role or explicitly set BACKUP_ALLOW_PARTIAL=true"))
	}
	// A warning can span multiple writes. Keep only enough to match its prefix.
	g.tail = append(g.tail[:0], data[max(0, len(data)-len(subscriptionWarning)+1):]...)
	return len(p), nil
}

// pipeline waits for every process, including a producer that closes stdout
// before reporting an error. Only its caller may signal EOF to the uploader.
func (r commandRunner) pipeline(ctx context.Context, stdout io.Writer, specs ...pipelineCommand) (result error) {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	if r.stderr != nil {
		if _, ok := r.stderr.(*synchronizedWriter); !ok {
			r.stderr = &synchronizedWriter{new(sync.Mutex), r.stderr}
		}
	}
	cmds := make([]*exec.Cmd, len(specs))
	type copyTask struct {
		reader *os.File
		writer *dumpHeaderWriter
		output *os.File
	}
	copies := make(map[int]copyTask)
	var parentOutputs []*os.File
	defer func() {
		for _, output := range parentOutputs {
			output.Close()
		}
		for _, task := range copies {
			task.reader.Close()
			if task.output != nil {
				task.output.Close()
			}
		}
	}()
	var secrets []*passphrasePipe
	defer func() {
		for _, secret := range secrets {
			result = errors.Join(result, secret.close())
		}
	}()
	var pipes []*os.File
	closePipes := func() {
		for _, pipe := range pipes {
			pipe.Close()
		}
		pipes = nil
	}
	defer closePipes()
	for i, spec := range specs {
		cmds[i] = r.command(ctx, spec.name, spec.args...)
		if spec.rejectMissingSubscriptions {
			cmd := cmds[i]
			// LC_ALL overrides LC_MESSAGES. Preserve its character encoding
			// for libpq users who select PGCLIENTENCODING=auto.
			if locale := os.Getenv("LC_ALL"); locale != "" {
				cmd.Env = append(cmd.Env, "LC_CTYPE="+locale)
			}
			cmd.Env = append(cmd.Env, "LC_ALL=", "LC_MESSAGES=C", "LANGUAGE=C", "PG_COLOR=never")
			guard := &subscriptionGuard{tail: []byte{'\n'}, cancel: cancel}
			cmd.Stderr = guard
			if r.stderr != nil {
				cmd.Stderr = io.MultiWriter(guard, r.stderr)
			}
		}
		if spec.passphrase != "" {
			secret, err := newPassphrasePipe(spec.passphrase)
			if err != nil {
				return err
			}
			secrets = append(secrets, secret)
			fd := strconv.Itoa(3 + len(cmds[i].ExtraFiles))
			cmds[i].ExtraFiles = append(cmds[i].ExtraFiles, secret.reader)
			cmds[i].Args = append([]string{cmds[i].Args[0], "--passphrase-fd", fd}, cmds[i].Args[1:]...)
			pipes = append(pipes, secret.reader)
		}
		// A slow upload may still be draining stdout after the child exits.
		// Cancellation closes the upload pipe; do not truncate it after WaitDelay.
		cmds[i].WaitDelay = 0
		if i > 0 {
			reader, writer, err := os.Pipe()
			if err != nil {
				return err
			}
			pipes = append(pipes, reader)
			if specs[i-1].customDump {
				parentOutputs = append(parentOutputs, writer)
			} else {
				pipes = append(pipes, writer)
			}
			cmds[i-1].Stdout, cmds[i].Stdin = writer, reader
		}
	}
	cmds[len(cmds)-1].Stdout = stdout
	for i, spec := range specs {
		if spec.customDump {
			reader, writer, err := os.Pipe()
			if err != nil {
				return err
			}
			task := copyTask{reader: reader, writer: &dumpHeaderWriter{dst: cmds[i].Stdout}}
			if i < len(specs)-1 {
				task.output = cmds[i].Stdout.(*os.File)
			}
			copies[i] = task
			cmds[i].Stdout = writer
			pipes = append(pipes, writer)
		}
	}
	var started []int
	// Start consumers first; OS pipes give backpressure without buffering dumps.
	for i := len(cmds) - 1; i >= 0; i-- {
		if err := cmds[i].Start(); err != nil {
			result = commandError(ctx, specs[i].name, err)
			cancel(result)
			break
		}
		started = append(started, i)
	}
	// Only children retain these descriptors, so early exits propagate EOF/EPIPE.
	closePipes()
	done := make(chan error, len(started))
	pending := len(started)
	for _, i := range started {
		if task, ok := copies[i]; ok {
			pending++
			go func() {
				// Tee the uncompressed TOC to the existing streaming pipeline. Any
				// parser read-ahead is already forwarded, so resume from the same
				// underlying reader without replaying or dropping buffered bytes.
				err := checkArchive(ctx, io.TeeReader(task.reader, task.writer), specs[i].rejectUnlogged)
				if err == nil {
					_, err = io.Copy(task.writer, task.reader)
				} else {
					err = fmt.Errorf("backup archive rejected before publication: %w", err)
				}
				task.reader.Close()
				if task.output != nil {
					// EOF belongs to the stream, not to the producer's exit.
					task.output.Close()
				}
				err = errors.Join(err, task.writer.complete())
				done <- err
			}()
		}
		go func() {
			err := commandError(ctx, specs[i].name, cmds[i].Wait())
			done <- err
		}()
	}
	for range pending {
		if err := <-done; err != nil {
			result = errors.Join(result, err)
			cancel(err)
		}
	}
	if cause := context.Cause(ctx); cause != nil && !errors.Is(result, cause) {
		return errors.Join(result, cause)
	}
	return result
}

func withoutEnv(environ []string, name string) []string {
	result := make([]string, 0, len(environ))
	for _, entry := range environ {
		if !strings.HasPrefix(entry, name+"=") {
			result = append(result, entry)
		}
	}
	return result
}

func acquireLock(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("open backup lock: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		file.Close()
		return nil, fmt.Errorf("Could not acquire backup lock; another backup or restore may already be running: %w", err)
	}
	// Close releases the lock. Never unlink it or explicitly unlock the shared
	// file description: any surviving child must retain its inherited lock.
	return file, nil
}

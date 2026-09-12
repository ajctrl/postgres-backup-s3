package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

type commandRunner struct {
	password string
	stderr   io.Writer
	lock     *os.File
}

func (r commandRunner) run(ctx context.Context, stdout io.Writer, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = stdout
	cmd.Stderr = r.stderr
	cmd.Env = append(withoutEnv(os.Environ(), "PGPASSWORD"), "PGPASSWORD="+r.password)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if r.lock != nil {
		// The dump/encryption child keeps the lock if the parent is killed abruptly.
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
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Never include argv here: it can contain a GPG passphrase.
		return fmt.Errorf("%s failed: %w", name, err)
	}
	return nil
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
		return nil, fmt.Errorf("Could not acquire backup lock; another backup may already be running: %w", err)
	}
	// Close releases the lock. Never unlink it or explicitly unlock the shared
	// file description: any surviving child must retain its inherited lock.
	return file, nil
}

package backup

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestRealGPGPassphrasePipeRoundTrip(t *testing.T) {
	gpg, err := exec.LookPath("gpg")
	if err != nil {
		t.Skip("GPG is not installed")
	}
	gpgconf, err := exec.LookPath("gpgconf")
	if err != nil {
		t.Skip("gpgconf is required to clean up the test agent")
	}
	home := t.TempDir()
	t.Setenv("GNUPGHOME", home)
	t.Setenv("PASSPHRASE", "must not be inherited")
	t.Cleanup(func() { exec.Command(gpgconf, "--homedir", home, "--kill", "gpg-agent").Run() })
	lock, err := acquireLock(filepath.Join(home, "lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	var stderr bytes.Buffer
	runner := commandRunner{stderr: &stderr, lock: lock}
	secret := " spaces \\ ' \" $ end "
	common := []string{"--batch", "--pinentry-mode", "loopback", "--agent-program", filepath.Join(filepath.Dir(gpg), "gpg-agent")}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	var encrypted bytes.Buffer
	archive := append(customArchiveFixture(16, tocFixture{kind: "DATABASE", section: 2, definition: "CREATE DATABASE app"}), []byte("payload")...)
	dump := filepath.Join(home, "plain.dump")
	if err := os.WriteFile(dump, archive, 0600); err != nil {
		t.Fatal(err)
	}
	if err := runner.pipeline(ctx, &encrypted,
		pipelineCommand{name: "cat", args: []string{dump}, customDump: true},
		pipelineCommand{name: gpg, passphrase: secret, args: append(append([]string{}, common...), "--symmetric", "--output", "-")}); err != nil {
		t.Fatalf("encrypt: %v\n%s", err, &stderr)
	}
	ciphertext, err := anonymousDump(home)
	if err != nil {
		t.Fatal(err)
	}
	defer ciphertext.Close()
	if _, err := ciphertext.WriteAt(encrypted.Bytes(), 0); err != nil {
		t.Fatal(err)
	}
	var plain bytes.Buffer
	if err := runner.pipeline(ctx, &plain, pipelineCommand{name: gpg, stdin: ciphertext, passphrase: secret,
		args: append(append([]string{}, common...), "--decrypt", "/dev/stdin")}); err != nil {
		t.Fatalf("decrypt: %v\n%s", err, &stderr)
	}
	if !bytes.Equal(plain.Bytes(), archive) {
		t.Fatal("encrypted round trip changed the plaintext")
	}
	// The persistent GPG agent must not retain the inherited operation lock.
	lock.Close()
	next, err := acquireLock(filepath.Join(home, "lock"))
	if err != nil {
		t.Fatalf("GPG agent retained the operation lock: %v", err)
	}
	next.Close()
}

package backup

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"
)

func TestAppAndChildShareLogLock(t *testing.T) {
	var log bytes.Buffer
	a := New(Config{}, nil, &log, &log)
	done := make(chan error, 1)
	go func() {
		done <- a.runner(nil).pipeline(t.Context(), io.Discard, pipelineCommand{name: "sh", args: []string{"-c", "i=0; while [ $i -lt 1000 ]; do echo warning >&2; i=$((i+1)); done"}})
	}()
	for range 1000 {
		io.WriteString(a.Out, "progress\n")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if strings.Count(log.String(), "warning\n") != 1000 || strings.Count(log.String(), "progress\n") != 1000 {
		t.Fatal("concurrent log writes lost output")
	}
}

func TestSubscriptionWarningAcrossWrites(t *testing.T) {
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	g := &subscriptionGuard{tail: []byte{'\n'}, cancel: cancel}
	line := subscriptionWarning[1:]
	io.WriteString(g, "other output containing "+line)
	if ctx.Err() != nil {
		t.Fatal("matched text inside a different message")
	}
	for i := range len(line) {
		g.Write([]byte{line[i]})
	}
	if ctx.Err() == nil {
		t.Fatal("split warning failed to stop the pipeline")
	}
}

func TestPipelineDrainsSlowOutputAfterProcessExit(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
	defer cancel()
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	done := make(chan error, 1)
	go func() {
		err := (commandRunner{stderr: io.Discard}).pipeline(ctx, writer, pipelineCommand{name: "sh", args: []string{"-c", "printf x"}})
		writer.CloseWithError(err)
		done <- err
	}()
	// Exceeds the five-second WaitDelay used for ordinary non-streaming commands.
	select {
	case <-time.After(6 * time.Second):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	body, err := io.ReadAll(reader)
	if err != nil || string(body) != "x" {
		t.Fatalf("slow output was truncated: %q, %v", body, err)
	}
	if err := <-done; err != nil {
		t.Fatalf("completed process failed while draining output: %v", err)
	}
}

package backup

import (
	"context"
	"io"
	"testing"
	"time"
)

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

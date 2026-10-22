package backup

import (
	"fmt"
	"io"
	"os"
	"strings"
)

func validatePassphrase(value string) error {
	if strings.ContainsAny(value, "\r\n\x00") {
		return fmt.Errorf("PASSPHRASE must not contain line breaks or NUL bytes")
	}
	return nil
}

type passphrasePipe struct {
	reader, writer *os.File
	done           chan error
}

func newPassphrasePipe(value string) (*passphrasePipe, error) {
	if err := validatePassphrase(value); err != nil {
		return nil, err
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("create passphrase pipe: %w", err)
	}
	p := &passphrasePipe{reader: reader, writer: writer, done: make(chan error, 1)}
	go func() {
		_, err := io.WriteString(writer, value+"\n")
		writer.Close()
		p.done <- err
	}()
	return p, nil
}

func (p *passphrasePipe) close() error {
	p.reader.Close()
	p.writer.Close() // Unblocks a writer when a child exits without consuming it.
	if err := <-p.done; err != nil {
		return fmt.Errorf("send passphrase to GPG: %w", err)
	}
	return nil
}

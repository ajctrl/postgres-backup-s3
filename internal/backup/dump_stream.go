package backup

import (
	"errors"
	"io"
)

var errInvalidDump = errors.New("pg_dump did not produce a PostgreSQL custom-format archive")

// Check the plaintext header before forwarding anything to GPG or S3. This is
// a format sanity check, not a substitute for waiting for pg_dump to succeed.
type dumpHeaderWriter struct {
	dst    io.Writer
	header []byte
	valid  bool
}

func (w *dumpHeaderWriter) Write(p []byte) (int, error) {
	consumed := 0
	if !w.valid {
		consumed = min(5-len(w.header), len(p))
		w.header = append(w.header, p[:consumed]...)
		if len(w.header) < 5 {
			return consumed, nil
		}
		if string(w.header) != "PGDMP" {
			return consumed, errInvalidDump
		}
		n, err := w.dst.Write(w.header)
		if err != nil {
			return consumed, err
		}
		if n != len(w.header) {
			return consumed, io.ErrShortWrite
		}
		w.valid = true
	}
	n, err := w.dst.Write(p[consumed:])
	return consumed + n, err
}

func (w *dumpHeaderWriter) complete() error {
	if !w.valid {
		return errInvalidDump
	}
	return nil
}

package backup

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// Validate the entire listing before deleting. Large cleanups spool batches of
// keys to a private temporary file so a long retention backlog fits in memory.
type expiredKeys struct {
	keys []string
	file *os.File
}

func (q *expiredKeys) add(key string) error {
	if len(q.keys) == 1000 {
		if q.file == nil {
			file, err := os.CreateTemp("", "postgres-backup-s3-retention-*")
			if err != nil {
				return fmt.Errorf("create retention key list: %w", err)
			}
			q.file = file
			// The executable runs on Linux. Unlink now so even SIGKILL cleans up.
			if err := os.Remove(file.Name()); err != nil {
				return fmt.Errorf("unlink retention key list: %w", err)
			}
		}
		if err := json.NewEncoder(q.file).Encode(q.keys); err != nil {
			return fmt.Errorf("write retention key list: %w", err)
		}
		q.keys = q.keys[:0]
	}
	q.keys = append(q.keys, key)
	return nil
}

func (q *expiredKeys) batches(visit func([]string) error) error {
	if q.file != nil {
		if _, err := q.file.Seek(0, io.SeekStart); err != nil {
			return err
		}
		decoder := json.NewDecoder(q.file)
		var batch []string
		for {
			err := decoder.Decode(&batch)
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("read retention key list: %w", err)
			}
			if err := visit(batch); err != nil {
				return err
			}
		}
	}
	if len(q.keys) > 0 {
		return visit(q.keys)
	}
	return nil
}

func (q *expiredKeys) close() {
	if q.file != nil {
		q.file.Close()
	}
}

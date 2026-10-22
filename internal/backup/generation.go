package backup

import (
	"crypto/rand"
	"fmt"
	"regexp"
	"time"
)

const timestampLayout = "2006-01-02T15:04:05"

var generationPattern = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}-[0-9a-f]{32}$`)

func newGeneration(now time.Time) (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", fmt.Errorf("generate backup generation ID: %w", err)
	}
	return fmt.Sprintf("%s-%x", now.UTC().Format(timestampLayout), id), nil
}

func validGeneration(name string) bool {
	if !generationPattern.MatchString(name) {
		return false
	}
	_, err := time.Parse(timestampLayout, name[:len(timestampLayout)])
	return err == nil
}

package backup

import (
	"strings"
	"testing"
	"time"
)

func TestGenerationUniquenessAtIdenticalTime(t *testing.T) {
	when := time.Date(2026, 10, 22, 19, 0, 0, 0, time.FixedZone("JST", 9*3600))
	seen := make(map[string]bool)
	for range 100 {
		name, err := newGeneration(when)
		if err != nil || !validGeneration(name) || !strings.HasPrefix(name, "2026-10-22T10:00:00-") || seen[name] {
			t.Fatalf("invalid or duplicate generation %q: %v", name, err)
		}
		seen[name] = true
	}
}

func TestGenerationNameValidation(t *testing.T) {
	for _, name := range []string{
		"2026-10-22T12:00:00", "2026-02-30T12:00:00-0123456789abcdef0123456789abcdef",
		"2026-10-22T12:00:00-0123456789abcdef0123456789abcdef.dump",
		"2026-10-22T12:00:00-0123456789abcdef0123456789abcdef\n",
		"../2026-10-22T12:00:00-0123456789abcdef0123456789abcdef",
		"2026-10-22T12:00:00-0123456789ABCDEF0123456789ABCDEF",
	} {
		if validGeneration(name) {
			t.Errorf("accepted invalid generation %q", name)
		}
	}
}

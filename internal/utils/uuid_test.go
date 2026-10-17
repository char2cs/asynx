package utils

import (
	"regexp"
	"testing"
)

var uuidV4 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestNewID_IsAUniqueVersion4UUID(t *testing.T) {
	seen := make(map[string]struct{})
	for range 1000 {
		id := NewID()
		if !uuidV4.MatchString(id) {
			t.Fatalf("NewID() = %q, not a version 4 UUID", id)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("NewID() repeated %q", id)
		}
		seen[id] = struct{}{}
	}
}

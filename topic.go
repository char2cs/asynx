package asynx

import (
	"regexp"
	"strings"
)

// Topic converts a human-readable event pattern into an anchored Go regex string.
//
// Pattern format: {{aggregate}}.{{action}}.{{id}}
//
// Segments are split with strings.SplitN(pattern, ".", 3) — the third segment
// (id) may contain dots and other regex metacharacters.
//
// Rules:
//   - Trailing * → .* (greedy — id segment can contain dots)
//   - Middle * → [^.]+ (segment-bounded — aggregate/action have no dots)
//   - Literal segments → regexp.QuoteMeta
//   - Result is anchored: ^...$
func Topic(pattern string) string {
	parts := strings.SplitN(pattern, ".", 3)
	for i, p := range parts {
		if p == "*" {
			if i == len(parts)-1 {
				parts[i] = ".*"
			} else {
				parts[i] = "[^.]+"
			}
		} else {
			parts[i] = regexp.QuoteMeta(p)
		}
	}
	return "^" + strings.Join(parts, `\.`) + "$"
}

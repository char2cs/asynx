package asynx

import (
	"regexp"
	"strings"
)

const topicSegments = 3

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
func Topic(
	pattern string,
) string {
	parts := strings.SplitN(
		pattern,
		".",
		topicSegments,
	)
	last := len(parts) - 1
	for i, p := range parts[:last] {
		parts[i] = innerSegmentPattern(p)
	}

	parts[last] = lastSegmentPattern(parts[last])

	return "^" + strings.Join(
		parts,
		`\.`,
	) + "$"
}

func innerSegmentPattern(
	segment string,
) string {
	if segment != "*" {
		return regexp.QuoteMeta(segment)
	}

	return "[^.]+"
}

func lastSegmentPattern(
	segment string,
) string {
	if segment != "*" {
		return regexp.QuoteMeta(segment)
	}

	return ".*"
}

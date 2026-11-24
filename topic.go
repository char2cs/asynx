package asynx

import (
	"regexp"
	"strings"
)

const topicSegments = 3

// Topic converts a readable event pattern into an anchored regular expression
// that matches dotted event names. The result is a pattern Subscribe accepts,
// and it is what Listen and SubscribeWait apply to their pattern argument.
//
// The pattern has the shape {{aggregate}}.{{action}}.{{id}}. It is split into
// at most three segments at the first two dots (strings.SplitN), so the last
// segment may itself contain dots and other regular expression metacharacters.
//
// The rules, applied per segment, are:
//   - A literal segment is escaped with regexp.QuoteMeta and matches itself
//     exactly.
//   - A segment that is exactly * before the last one matches one or more
//     characters other than a dot, [^.]+, so it stays within its own segment.
//   - A last segment that is exactly * matches anything, .*, dots included.
//   - The result is anchored with ^ and $, so it must match the whole name.
//
// For example, "Order.*" becomes ^Order\..*$ and "*.Created.*" becomes
// ^[^.]+\.Created\..*$. A pattern with fewer than three segments is converted
// the same way, and the empty pattern becomes ^$. Topic does not validate its
// input and never fails.
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

package ids

import (
	"crypto/rand"
	"fmt"
)

const (
	versionMask    = 0x0f
	versionFour    = 0x40
	variantMask    = 0x3f
	variantRFC4122 = 0x80
)

// New returns a random version 4 UUID in the RFC 4122 variant, formatted as
// five lowercase hex groups separated by dashes (8-4-4-4-12).
//
// It cannot fail: since Go 1.24 crypto/rand.Read never returns an error, it
// crashes the program irrecoverably if the operating system's randomness
// source is unavailable, so there is no failure for New to report and it never
// returns an empty or partly random ID. It is safe for concurrent use.
func New() string {
	var b [16]byte
	rand.Read(b[:])

	b[6] = (b[6] & versionMask) | versionFour
	b[8] = (b[8] & variantMask) | variantRFC4122

	return fmt.Sprintf(
		"%x-%x-%x-%x-%x",
		b[0:4],
		b[4:6],
		b[6:8],
		b[8:10],
		b[10:],
	)
}

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

// New returns a random UUID v4 string.
func New() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return ""
	}

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

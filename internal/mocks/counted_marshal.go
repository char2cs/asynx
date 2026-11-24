package mocks

import (
	"errors"
)

// CountedMarshal counts MarshalJSON calls and returns an error on call
// number FailAt. All other calls return {}. Use as a pointer type parameter
// (e.g. Writer[*CountedMarshal]) so the counter is shared across calls.
type CountedMarshal struct {
	N      int
	FailAt int
}

func (c *CountedMarshal) MarshalJSON() ([]byte, error) {
	c.N++
	if c.N == c.FailAt {
		return nil,
			errors.New("marshal error")
	}

	return []byte(`{}`),
		nil
}

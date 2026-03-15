package mocks

import "errors"

// ErrMarshal always fails MarshalJSON. Used as a generic type parameter to
// trigger json.Marshal error paths in tests.
type ErrMarshal struct{}

func (e ErrMarshal) MarshalJSON() ([]byte, error) {
	return nil, errors.New("marshal error")
}

// BadUnmarshal marshals successfully (returns {}) but always fails
// UnmarshalJSON. Used as a generic type parameter to trigger
// json.Unmarshal error paths in tests.
type BadUnmarshal struct{}

func (b BadUnmarshal) MarshalJSON() ([]byte, error) { return []byte(`{}`), nil }
func (b *BadUnmarshal) UnmarshalJSON([]byte) error  { return errors.New("unmarshal error") }

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
		return nil, errors.New("marshal error")
	}
	return []byte(`{}`), nil
}

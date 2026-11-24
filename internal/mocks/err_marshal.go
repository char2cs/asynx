package mocks

import (
	"errors"
)

// ErrMarshal always fails MarshalJSON. Used as a generic type parameter to
// trigger json.Marshal error paths in tests.
type ErrMarshal struct{}

func (e ErrMarshal) MarshalJSON() ([]byte, error) {
	return nil,
		errors.New("marshal error")
}

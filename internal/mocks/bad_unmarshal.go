package mocks

import (
	"errors"
)

// BadUnmarshal marshals successfully (returns {}) but always fails
// UnmarshalJSON. Used as a generic type parameter to trigger
// json.Unmarshal error paths in tests.
type BadUnmarshal struct{}

func (b BadUnmarshal) MarshalJSON() ([]byte, error) {
	return []byte(`{}`),
		nil
}

func (b *BadUnmarshal) UnmarshalJSON(
	[]byte,
) error {
	return errors.New("unmarshal error")
}

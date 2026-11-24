package mocks

import (
	"encoding/json"
	"testing"
)

func TestErrMarshal_MarshalJSON_ReturnsError(
	t *testing.T,
) {
	_, err := json.Marshal(ErrMarshal{})
	if err == nil {
		t.Fatal("expected marshal error, got nil")
	}
}

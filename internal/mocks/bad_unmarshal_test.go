package mocks

import (
	"encoding/json"
	"testing"
)

func TestBadUnmarshal_MarshalJSON_Succeeds(
	t *testing.T,
) {
	b, err := json.Marshal(BadUnmarshal{})
	if err != nil {
		t.Fatalf(
			"MarshalJSON: %v",
			err,
		)
	}

	if string(b) != "{}" {
		t.Errorf(
			"MarshalJSON = %q, want {}",
			b,
		)
	}
}

func TestBadUnmarshal_UnmarshalJSON_ReturnsError(
	t *testing.T,
) {
	var v BadUnmarshal
	if err := json.Unmarshal(
		[]byte(`{}`),
		&v,
	); err == nil {
		t.Fatal("expected unmarshal error, got nil")
	}
}

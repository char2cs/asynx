package mocks

import (
	"encoding/json"
	"testing"
)

func TestCountedMarshal_SucceedsBeforeFailAt(
	t *testing.T,
) {
	cm := &CountedMarshal{FailAt: 3}

	for i := 1; i <= 2; i++ {
		b, err := json.Marshal(cm)
		if err != nil {
			t.Fatalf(
				"call %d: unexpected error: %v",
				i,
				err,
			)
		}

		if string(b) != "{}" {
			t.Errorf(
				"call %d: got %q, want {}",
				i,
				b,
			)
		}
	}
}

func TestCountedMarshal_FailsAtFailAt(
	t *testing.T,
) {
	cm := &CountedMarshal{FailAt: 2}

	json.Marshal(cm)

	_, err := json.Marshal(cm)
	if err == nil {
		t.Fatal("expected marshal error on call FailAt, got nil")
	}
}

func TestCountedMarshal_SucceedsAfterFailAt(
	t *testing.T,
) {
	cm := &CountedMarshal{FailAt: 1}

	json.Marshal(cm)

	_, err := json.Marshal(cm)
	if err != nil {
		t.Errorf(
			"call after FailAt: %v (want nil)",
			err,
		)
	}
}

func TestCountedMarshal_NCountsAllCalls(
	t *testing.T,
) {
	cm := &CountedMarshal{FailAt: 99}
	for range 5 {
		json.Marshal(cm)
	}

	if cm.N != 5 {
		t.Errorf(
			"N = %d, want 5",
			cm.N,
		)
	}
}

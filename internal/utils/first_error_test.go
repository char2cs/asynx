package utils

import (
	"errors"
	"testing"
)

func TestFirstError_StopsAtTheFirstFailure(t *testing.T) {
	boom := errors.New("boom")
	calls := 0

	err := FirstError(
		[]int{1, 2, 3},
		func(n int) error {
			calls++
			if n == 2 {
				return boom
			}
			return nil
		},
	)

	if !errors.Is(err, boom) {
		t.Fatalf("FirstError() = %v, want %v", err, boom)
	}
	if calls != 2 {
		t.Fatalf("try ran %d times, want 2", calls)
	}
}

func TestFirstError_ReturnsNilWhenEveryCallSucceeds(t *testing.T) {
	err := FirstError(
		[]int{1, 2, 3},
		func(int) error { return nil },
	)

	if err != nil {
		t.Fatalf("FirstError() = %v, want nil", err)
	}
}

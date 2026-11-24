package firsterror

import (
	"errors"
	"testing"
)

func TestFirstError_StopsAtTheFirstFailure(
	t *testing.T,
) {
	boom := errors.New("boom")
	calls := 0

	err := Of(
		[]int{1, 2, 3},
		func(
			n int,
		) error {
			calls++
			if n == 2 {
				return boom
			}

			return nil
		},
	)
	if !errors.Is(
		err,
		boom,
	) {
		t.Fatalf(
			"Of() = %v, want %v",
			err,
			boom,
		)
	}

	if calls != 2 {
		t.Fatalf(
			"try ran %d times, want 2",
			calls,
		)
	}
}

func TestFirstError_ReturnsNilWhenEveryCallSucceeds(
	t *testing.T,
) {
	err := Of(
		[]int{1, 2, 3},
		func(
			int,
		) error {
			return nil
		},
	)
	if err != nil {
		t.Fatalf(
			"Of() = %v, want nil",
			err,
		)
	}
}

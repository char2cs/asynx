package firsterror

import (
	"errors"
	"testing"
)

type failingTry struct {
	failOn int
	err    error
	calls  int
}

func (f *failingTry) try(
	n int,
) error {
	f.calls++
	if n == f.failOn {
		return f.err
	}

	return nil
}

func succeed(
	int,
) error {
	return nil
}

func TestFirstError_StopsAtTheFirstFailure(
	t *testing.T,
) {
	boom := errors.New("boom")
	f := &failingTry{failOn: 2, err: boom}

	err := Of(
		[]int{1, 2, 3},
		f.try,
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

	if f.calls != 2 {
		t.Fatalf(
			"try ran %d times, want 2",
			f.calls,
		)
	}
}

func TestFirstError_ReturnsNilWhenEveryCallSucceeds(
	t *testing.T,
) {
	err := Of(
		[]int{1, 2, 3},
		succeed,
	)
	if err != nil {
		t.Fatalf(
			"Of() = %v, want nil",
			err,
		)
	}
}

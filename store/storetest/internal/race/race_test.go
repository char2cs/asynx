package race

import (
	"errors"
	"slices"
	"sync"
	"testing"
	"time"
)

const timeout = 5 * time.Second

func TestAll_ReturnsEachLanesErrorByIndex(
	t *testing.T,
) {
	errThird := errors.New("third")
	run := func(
		i int,
	) error {
		return []error{nil, nil, errThird}[i]
	}

	got := All(
		3,
		run,
	)
	if !slices.Equal(
		got,
		[]error{nil, nil, errThird},
	) {
		t.Fatalf(
			"All = %v, want the error of each lane at its own index",
			got,
		)
	}
}

func TestAll_RunsTheLanesConcurrently(
	t *testing.T,
) {
	const lanesCount = 8
	var meet sync.WaitGroup
	meet.Add(lanesCount)
	run := func(
		int,
	) error {
		meet.Done()
		meet.Wait()
		return nil
	}
	finished := make(chan struct{})
	go func() {
		All(
			lanesCount,
			run,
		)
		close(finished)
	}()

	select {
	case <-finished:
	case <-time.After(timeout):
		t.Fatal("the lanes never all ran at once, so they did not overlap")
	}
}

func TestAll_ZeroLanesReturnsNothing(
	t *testing.T,
) {
	run := func(
		int,
	) error {
		t.Error("run was called for zero lanes")
		return nil
	}

	if got := All(
		0,
		run,
	); len(got) != 0 {
		t.Fatalf(
			"All(0) = %v, want no results",
			got,
		)
	}
}

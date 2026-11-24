package aggregates_test

import (
	"testing"

	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/aggregates"
)

func TestNew_KnowsNoAggregate(
	t *testing.T,
) {
	r, _ := newRegistry()
	if _, ok := r.Begin(liveItem(
		"a",
		1,
	)); ok {
		t.Fatal("Begin on an empty registry found a job")
	}

	if len(r.Parks()) != 0 {
		t.Fatalf(
			"Parks = %v, want none",
			r.Parks(),
		)
	}

	if r.Unrun(
		"a",
		1,
	) != nil {
		t.Fatal("Unrun of an unknown aggregate is not nil")
	}
}

func TestNew_AcceptsNilHooks(
	t *testing.T,
) {
	r := aggregates.New(aggregates.Hooks[*fakeItem]{})
	admission, err := r.Enqueue(liveItem(
		"a",
		1,
	))
	if err != nil ||
		!closed(admission.Turn) {
		t.Fatalf(
			"Enqueue = %+v, %v",
			admission,
			err,
		)
	}
}

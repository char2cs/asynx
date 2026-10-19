package aggregates_test

import (
	"testing"

	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/aggregates"
)

func TestNew_KnowsNoAggregate(t *testing.T) {
	r, _ := newRegistry()
	if _, _, ok := r.Pop("a"); ok {
		t.Fatal("Pop on an empty registry found a job")
	}
	if r.Wake("a") != nil {
		t.Fatal("Wake of an unknown aggregate is not nil")
	}
	if len(r.Parks()) != 0 {
		t.Fatalf("Parks = %v, want none", r.Parks())
	}
	if r.Unrun("a", 1) != nil {
		t.Fatal("Unrun of an unknown aggregate is not nil")
	}
	if !r.Retire("a") {
		t.Fatal("Retire of an unknown aggregate refused")
	}
}

func TestNew_AcceptsNilHooks(t *testing.T) {
	r := aggregates.New(aggregates.Hooks[*fakeItem]{})
	admission, err := r.Enqueue(liveItem("a", 1))
	if err != nil || !admission.Spawned {
		t.Fatalf("Enqueue = %+v, %v", admission, err)
	}
}

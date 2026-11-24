package aggregates

import (
	"testing"
)

type stubItem struct {
	version int64
}

// AggregateID names the one aggregate every stubItem belongs to.
func (stubItem) AggregateID() string {
	return "a"
}

// Version returns the version the stubItem was built with.
func (s stubItem) Version() int64 {
	return s.version
}

// MarkUnrun ignores the settlement: the record never settles its items.
func (stubItem) MarkUnrun(
	error,
) {
}

// Complete ignores the completion: the record never completes its items.
func (stubItem) Complete(
	error,
) {
}

// Acked reports an acknowledged item, which a park holds back.
func (stubItem) Acked() bool {
	return true
}

func TestRecord_IsHeadOnlyForTheOldestQueuedJob(
	t *testing.T,
) {
	first := &stubItem{version: 1}
	second := &stubItem{version: 2}
	rec := &record[*stubItem]{jobs: []*stubItem{first, second}}
	if !rec.isHead(first) ||
		rec.isHead(second) {
		t.Fatal("isHead is not true for exactly the oldest queued job")
	}
}

func TestRecord_NoJobIsTheHeadOfAnEmptyQueue(
	t *testing.T,
) {
	rec := &record[*stubItem]{}
	if rec.isHead(&stubItem{version: 1}) {
		t.Fatal("isHead of an empty queue was true")
	}
}

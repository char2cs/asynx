package aggregates_test

import "sync"

type fakeItem struct {
	mu          sync.Mutex
	aggregateID string
	version     int64
	durable     bool
	unrun       error
	unrunCalls  int
	err         error
	completions int
}

func durableItem(
	aggregateID string,
	version int64,
) *fakeItem {
	return &fakeItem{aggregateID: aggregateID, version: version, durable: true}
}

func liveItem(
	aggregateID string,
	version int64,
) *fakeItem {
	return &fakeItem{aggregateID: aggregateID, version: version}
}

func (f *fakeItem) AggregateID() string { return f.aggregateID }

func (f *fakeItem) Version() int64 { return f.version }

func (f *fakeItem) Acked() bool { return f.durable }

func (f *fakeItem) MarkUnrun(
	err error,
) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unrunCalls++
	f.unrun = err
}

func (f *fakeItem) Complete(
	err error,
) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.completions++
	f.err = err
}

func (f *fakeItem) state() (unrun error, completions int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.unrun, f.completions, f.err
}

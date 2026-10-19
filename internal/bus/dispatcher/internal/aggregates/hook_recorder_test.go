package aggregates_test

import (
	"sync"

	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/aggregates"
)

type hookRecorder struct {
	mu       sync.Mutex
	spawned  []string
	admitted []*fakeItem
}

func newRegistry() (aggregates.Registry[*fakeItem], *hookRecorder) {
	rec := &hookRecorder{}
	return aggregates.New(aggregates.Hooks[*fakeItem]{Spawn: rec.spawn, Admit: rec.admit}), rec
}

func (h *hookRecorder) spawn(
	aggregateID string,
) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.spawned = append(h.spawned, aggregateID)
}

func (h *hookRecorder) admit(
	job *fakeItem,
) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.admitted = append(h.admitted, job)
}

func (h *hookRecorder) spawns() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.spawned)
}

func (h *hookRecorder) admits() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.admitted)
}

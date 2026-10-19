package workers

import (
	"slices"
	"sync"
)

type journal struct {
	mu      sync.Mutex
	entries []string
}

func (j *journal) add(
	entry string,
) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.entries = append(j.entries, entry)
}

func (j *journal) list() []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return slices.Clone(j.entries)
}

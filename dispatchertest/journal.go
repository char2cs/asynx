package dispatchertest

import (
	"fmt"
	"sync"

	"github.com/char2cs/asynx/models"
)

type journal struct {
	mu      sync.Mutex
	entries []string
}

func (j *journal) add(
	e models.Event[Aggregate],
) {
	j.note(fmt.Sprintf("%s/%d", e.AggregateID, e.Version))
}

func (j *journal) note(
	entry string,
) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.entries = append(j.entries, entry)
}

func (j *journal) list() []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return append([]string(nil), j.entries...)
}

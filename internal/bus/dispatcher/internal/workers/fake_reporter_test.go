package workers

import (
	"slices"
	"sync"

	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/jobs"
)

type fakeReporter struct {
	journal *journal
	mu      sync.Mutex
	errs    []error
}

func (r *fakeReporter) PublishError(
	_ jobs.Job[int],
	err error,
) {
	r.journal.add("report")
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errs = append(r.errs, err)
}

func (r *fakeReporter) DeliveryError(
	jobs.Job[int],
	error,
	int,
) {
}

func (r *fakeReporter) Panic(
	jobs.Job[int],
	any,
) {
}

func (r *fakeReporter) published() []error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.errs)
}

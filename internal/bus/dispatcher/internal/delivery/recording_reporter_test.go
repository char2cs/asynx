package delivery

import (
	"maps"
	"slices"
	"sync"

	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/aggregates"
	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/jobs"
)

type recordingReporter struct {
	mu       sync.Mutex
	registry aggregates.Registry[jobs.Job[int]]
	reports  []report
	panics   []any
}

func (r *recordingReporter) record(
	kind string,
	err error,
) {
	parked := slices.Sorted(maps.Keys(r.registry.Parks()))
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reports = append(r.reports, report{kind: kind, err: err, parked: parked})
}

func (r *recordingReporter) PublishError(
	_ jobs.Job[int],
	err error,
) {
	r.record("publish", err)
}

func (r *recordingReporter) DeliveryError(
	_ jobs.Job[int],
	err error,
	_ int,
) {
	r.record("delivery", err)
}

func (r *recordingReporter) Panic(
	_ jobs.Job[int],
	recovered any,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.panics = append(r.panics, recovered)
}

func (r *recordingReporter) all() []report {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.reports)
}

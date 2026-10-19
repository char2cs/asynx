package tracker_test

import (
	"sync"

	asynxmd "github.com/char2cs/asynx/models"

	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/jobs"
)

type observedJob struct {
	jobs.Job[string]
	once  sync.Once
	asked chan struct{}
}

func observe(
	job jobs.Job[string],
) *observedJob {
	return &observedJob{Job: job, asked: make(chan struct{})}
}

func (j *observedJob) Ticket() asynxmd.Ticket {
	j.once.Do(func() { close(j.asked) })
	return j.Job.Ticket()
}

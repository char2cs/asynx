package workers

import (
	"fmt"
	"sync"
	"time"

	asynxmd "github.com/char2cs/asynx/models"

	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/aggregates"
	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/delivery"
	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/jobs"
	"github.com/char2cs/asynx/internal/bus/dispatcher/internal/reporter"
)

type aggregatePool[T any] struct {
	bus         asynxmd.Bus[T]
	registry    aggregates.Registry[jobs.Job[T]]
	deliverer   delivery.Deliverer[T]
	reports     reporter.Reporter[T]
	idleTimeout time.Duration
	stop        chan struct{}
	wg          sync.WaitGroup
}

func (p *aggregatePool[T]) Spawn(
	aggregateID string,
) {
	p.wg.Add(1)
	go p.work(aggregateID)
}

func (p *aggregatePool[T]) Stop() {
	close(p.stop)
}

func (p *aggregatePool[T]) Wait() {
	p.wg.Wait()
}

func (p *aggregatePool[T]) work(
	aggregateID string,
) {
	defer p.wg.Done()
	wake := p.registry.Wake(aggregateID)
	for p.cycle(aggregateID, wake) {
	}
}

func (p *aggregatePool[T]) cycle(
	aggregateID string,
	wake <-chan struct{},
) bool {
	p.drain(aggregateID)
	return !p.await(aggregateID, wake)
}

func (p *aggregatePool[T]) await(
	aggregateID string,
	wake <-chan struct{},
) bool {
	idle := time.NewTimer(p.idleTimeout)
	defer idle.Stop()
	select {
	case <-wake:
		return false
	case <-p.stop:
		p.drain(aggregateID)
		return true
	case <-idle.C:
		return p.registry.Retire(aggregateID)
	}
}

func (p *aggregatePool[T]) drain(
	aggregateID string,
) {
	for p.next(aggregateID) {
	}
}

func (p *aggregatePool[T]) next(
	aggregateID string,
) bool {
	job, halt, ok := p.registry.Pop(aggregateID)
	if !ok {
		return false
	}
	p.handle(aggregateID, halt, job)
	return true
}

func (p *aggregatePool[T]) handle(
	aggregateID string,
	halt <-chan struct{},
	job jobs.Job[T],
) {
	var outcome error
	defer finish(job, &outcome)
	defer p.recoverDispatcherPanic(job, &outcome)
	outcome = p.deliver(aggregateID, halt, job)
}

func (p *aggregatePool[T]) recoverDispatcherPanic(
	job jobs.Job[T],
	outcome *error,
) {
	recovered := recover()
	if recovered == nil {
		return
	}
	err := fmt.Errorf("asynx: dispatcher panic: %v", recovered)
	p.reports.PublishError(job, err)
	*outcome = err
}

func (p *aggregatePool[T]) deliver(
	aggregateID string,
	halt <-chan struct{},
	job jobs.Job[T],
) error {
	if p.heldByPark(aggregateID, halt, job) {
		return asynxmd.ErrDeliveryParked
	}
	p.publishLive(job)
	if !job.Durable() {
		return nil
	}
	err := p.deliverer.Deliver(aggregateID, halt, job)
	if err != nil {
		return err
	}
	return p.ack(aggregateID, halt, job)
}

func (p *aggregatePool[T]) heldByPark(
	aggregateID string,
	halt <-chan struct{},
	job jobs.Job[T],
) bool {
	if !job.Acked() {
		return false
	}
	return p.registry.SettleIfParked(aggregateID, halt, job)
}

func (p *aggregatePool[T]) publishLive(
	job jobs.Job[T],
) {
	if p.bus == nil || job.SkipLive() {
		return
	}
	err := p.publish(job)
	if err == nil {
		return
	}
	p.reports.PublishError(job, err)
}

func (p *aggregatePool[T]) publish(
	job jobs.Job[T],
) (err error) {
	defer recoverBusPanic(&err)
	return p.bus.PublishSync(job.Context(), job.Event())
}

func (p *aggregatePool[T]) ack(
	aggregateID string,
	halt <-chan struct{},
	job jobs.Job[T],
) error {
	ack := job.Ack()
	if ack == nil {
		return nil
	}
	err := ack(job.Context())
	if err == nil {
		return nil
	}
	p.deliverer.ParkThenReport(aggregateID, halt, job, err, func() {
		p.reports.PublishError(job, err)
	})
	return err
}

func finish[T any](
	job jobs.Job[T],
	outcome *error,
) {
	job.MarkSettled()
	job.Complete(*outcome)
}

func recoverBusPanic(
	err *error,
) {
	recovered := recover()
	if recovered == nil {
		return
	}
	*err = fmt.Errorf("asynx: bus panic: %v", recovered)
}

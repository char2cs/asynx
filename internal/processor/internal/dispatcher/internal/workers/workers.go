package workers

import (
	"context"
	"fmt"
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/aggregates"
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/delivery"
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/jobs"
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/reporter"
	asynxmd "github.com/char2cs/asynx/models"
)

// New creates a Pool whose runs begin their jobs in registry, publish them
// through bus (nil publishes nothing), run their durable delivery through
// deliverer and report errors through reports.
func New[T any](
	bus asynxmd.Bus[T],
	registry aggregates.Registry[jobs.Job[T]],
	deliverer delivery.Deliverer[T],
	reports reporter.Reporter[T],
) Pool[T] {
	return &aggregatePool[T]{
		bus:       bus,
		registry:  registry,
		deliverer: deliverer,
		reports:   reports,
	}
}

type aggregatePool[T any] struct {
	bus       asynxmd.Bus[T]
	registry  aggregates.Registry[jobs.Job[T]]
	deliverer delivery.Deliverer[T]
	reports   reporter.Reporter[T]
}

// Run waits for turn, delivers job and hands the turn on through done.
func (p *aggregatePool[T]) Run(
	ctx context.Context,
	job jobs.Job[T],
	turn <-chan struct{},
	done chan<- struct{},
) {
	<-turn
	defer p.registry.Finish(
		job.AggregateID(),
		done,
	)
	halt, ok := p.registry.Begin(job)
	if !ok {
		return
	}

	p.handle(
		ctx,
		job.AggregateID(),
		halt,
		job,
	)
}

func (p *aggregatePool[T]) handle(
	ctx context.Context,
	aggregateID string,
	halt <-chan struct{},
	job jobs.Job[T],
) {
	var outcome error
	defer finish(
		job,
		&outcome,
	)
	defer p.recoverDispatcherPanic(
		ctx,
		job,
		&outcome,
	)
	outcome = p.deliver(
		ctx,
		aggregateID,
		halt,
		job,
	)
}

func (p *aggregatePool[T]) recoverDispatcherPanic(
	ctx context.Context,
	job jobs.Job[T],
	outcome *error,
) {
	recovered := recover()
	if recovered == nil {
		return
	}

	err := fmt.Errorf(
		"asynx: dispatcher panic: %v",
		recovered,
	)
	p.reports.PublishError(
		ctx,
		job,
		err,
	)
	*outcome = err
}

func (p *aggregatePool[T]) deliver(
	ctx context.Context,
	aggregateID string,
	halt <-chan struct{},
	job jobs.Job[T],
) error {
	if p.heldByPark(
		aggregateID,
		halt,
		job,
	) {
		return asynxmd.ErrDeliveryParked
	}

	p.publishLive(
		ctx,
		job,
	)
	if !job.Durable() {
		return nil
	}

	err := p.deliverer.Deliver(
		ctx,
		aggregateID,
		halt,
		job,
	)
	if err != nil {
		return err
	}

	return p.ack(
		ctx,
		aggregateID,
		halt,
		job,
	)
}

func (p *aggregatePool[T]) heldByPark(
	aggregateID string,
	halt <-chan struct{},
	job jobs.Job[T],
) bool {
	if !job.Acked() {
		return false
	}

	return p.registry.SettleIfParked(
		aggregateID,
		halt,
		job,
	)
}

func (p *aggregatePool[T]) publishLive(
	ctx context.Context,
	job jobs.Job[T],
) {
	if p.bus == nil ||
		job.SkipLive() {
		return
	}

	err := p.publish(
		ctx,
		job,
	)
	if err == nil {
		return
	}

	p.reports.PublishError(
		ctx,
		job,
		err,
	)
}

func (p *aggregatePool[T]) publish(
	ctx context.Context,
	job jobs.Job[T],
) (err error) {
	defer recoverBusPanic(&err)
	return p.bus.PublishSync(
		ctx,
		job.Event(),
	)
}

func (p *aggregatePool[T]) ack(
	ctx context.Context,
	aggregateID string,
	halt <-chan struct{},
	job jobs.Job[T],
) error {
	ack := job.Ack()
	if ack == nil {
		return nil
	}

	err := ack(ctx)
	if err == nil {
		return nil
	}

	report := func(
		ctx context.Context,
	) {
		p.reports.PublishError(
			ctx,
			job,
			err,
		)
	}
	p.deliverer.ParkThenReport(
		ctx,
		aggregateID,
		halt,
		job,
		err,
		report,
	)
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

	*err = fmt.Errorf(
		"asynx: bus panic: %v",
		recovered,
	)
}

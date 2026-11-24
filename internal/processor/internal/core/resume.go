package core

import (
	"context"
	"errors"
	"fmt"
	asynxmd "github.com/char2cs/asynx/models"
	"maps"
	"slices"
)

// Paused lists, sorted, the aggregates whose durable delivery is parked: a
// delivery gave up (see ErrDeliveryParked), or a Forget could not deliver its
// marker.
func (p *Processor[T]) Paused() []string {
	if p.dispatcher == nil {
		return nil
	}

	return slices.Sorted(maps.Keys(p.dispatcher.Parks()))
}

// Inspect reports the aggregate's delivery status as the dispatcher sees it:
// whether it is parked, the park's generation, the error that parked it and
// the versions the park left undelivered. Without a dispatcher it is the zero
// DeliveryStatus.
func (p *Processor[T]) Inspect(
	aggregateID string,
) asynxmd.DeliveryStatus {
	if p.dispatcher == nil {
		return asynxmd.DeliveryStatus{}
	}

	return p.dispatcher.Inspect(aggregateID)
}

// Resume redelivers the parked aggregates, to the durable subscribers. For each
// it lifts the park and re-enqueues the aggregate's pending deliveries, in
// order, under the aggregate's command lock so no live event overtakes them.
// The pending deliveries include a deletion marker left by a failed Forget,
// which therefore goes out before anything that followed it. Call it once the
// cause (a bad handler, a downstream outage) is fixed.
//
// The delivery log is read once for all of them, before any park is lifted and
// without holding a lock; the store has no per-aggregate query. Each aggregate
// is then redelivered under its own lock, never holding another one, so a
// command that sends to a second aggregate from inside its validation cannot
// deadlock with a resume. Events committed after the read are covered because
// the dispatcher remembers the ones a park left undelivered.
//
// An aggregate that cannot be redelivered stays parked and the others are still
// resumed; the errors are joined. If the log cannot be read nothing changes.
func (p *Processor[T]) Resume(
	ctx context.Context,
	batch int,
) error {
	if p.dispatcher == nil {
		return nil
	}

	if batch <= 0 {
		batch = defaultRecoverBatch
	}

	parks := p.dispatcher.Parks()
	halted := slices.Sorted(maps.Keys(parks))
	if len(halted) == 0 {
		return nil
	}

	backlogs, _, err := p.pendingByAggregate(
		ctx,
		batch,
		halted,
	)
	if err != nil {
		return err
	}

	return p.resumeAll(
		ctx,
		parks,
		halted,
		backlogs,
	)
}

func (p *Processor[T]) resumeAll(
	ctx context.Context,
	parks map[string]uint64,
	halted []string,
	backlogs map[string][]asynxmd.Delivery,
) error {
	var failures []error
	for i := 0; i < len(halted) &&
		ctx.Err() == nil; i++ {
		failures = append(
			failures,
			p.resumeAggregate(
				ctx,
				halted[i],
				parks[halted[i]],
				backlogs[halted[i]],
			),
		)
	}

	return errors.Join(failures...)
}

func (p *Processor[T]) resumeAggregate(
	ctx context.Context,
	agg string,
	park uint64,
	scanned []asynxmd.Delivery,
) error {
	return p.executor.WithLock(
		ctx,
		agg,
		p.redeliveringParked(
			agg,
			park,
			scanned,
		),
	)
}

func (p *Processor[T]) redeliveringParked(
	agg string,
	park uint64,
	scanned []asynxmd.Delivery,
) func(context.Context) error {
	return func(
		ctx context.Context,
	) error {
		return p.redeliverParked(
			ctx,
			agg,
			park,
			scanned,
		)
	}
}

func (p *Processor[T]) redeliverParked(
	ctx context.Context,
	agg string,
	park uint64,
	scanned []asynxmd.Delivery,
) error {
	lifted, undelivered := p.dispatcher.ResumeUndelivered(
		agg,
		park,
	)
	if !lifted {
		return nil
	}

	remaining, err := p.redeliver(
		ctx,
		agg,
		withUndelivered(
			agg,
			scanned,
			undelivered,
		),
	)
	if err == nil {
		return nil
	}

	p.dispatcher.Park(
		agg,
		err,
		stillUndelivered(
			undelivered,
			remaining,
		)...,
	)
	return fmt.Errorf(
		"asynx: resume %s: %w",
		agg,
		err,
	)
}

func withUndelivered(
	agg string,
	scanned []asynxmd.Delivery,
	undelivered []int64,
) []asynxmd.Delivery {
	backlog := slices.Clone(scanned)
	for _, v := range undelivered {
		backlog = append(
			backlog,
			asynxmd.Delivery{AggregateID: agg, Version: v},
		)
	}

	return backlog
}

func stillUndelivered(
	undelivered []int64,
	remaining []int64,
) []int64 {
	return slices.DeleteFunc(
		slices.Clone(undelivered),
		absentFrom(remaining),
	)
}

func absentFrom(
	sorted []int64,
) func(int64) bool {
	return func(
		v int64,
	) bool {
		_, found := slices.BinarySearch(
			sorted,
			v,
		)
		return !found
	}
}

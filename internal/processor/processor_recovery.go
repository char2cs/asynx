package processor

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/char2cs/asynx/internal/utils"
	asynxmd "github.com/char2cs/asynx/models"
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
		batch = 100
	}

	parks := p.dispatcher.Parks()
	halted := slices.Sorted(maps.Keys(parks))
	if len(halted) == 0 {
		return nil
	}

	wanted := make(
		map[string]struct{},
		len(halted),
	)
	for _, agg := range halted {
		wanted[agg] = struct{}{}
	}

	backlogs, _, err := p.pendingByAggregate(
		ctx,
		batch,
		func(
			id string,
		) bool {
			_, isParked := wanted[id]
			return isParked
		},
	)
	if err != nil {
		return err
	}

	var errs []error
	_ = slices.IndexFunc(
		halted,
		func(
			agg string,
		) bool {
			errs = append(
				errs,
				p.resumeAggregate(
					ctx,
					agg,
					parks[agg],
					backlogs[agg],
				),
			)
			return ctx.Err() != nil
		},
	)
	return errors.Join(errs...)
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
		func() error {
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
		},
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
		func(
			v int64,
		) bool {
			_, notEnqueued := slices.BinarySearch(
				remaining,
				v,
			)
			return !notEnqueued
		},
	)
}

// Recover re-enqueues every unacknowledged delivery, in enqueue order, on the
// per-aggregate dispatcher, for the durable subscribers (live subscribers never
// see what happened while the process was down). It returns once they are all
// queued, not once they are handled: a handler that keeps failing must not stop
// the process from starting. It takes no aggregate lock, so it must run before
// any command is sent, as Build does: then a live event enqueues behind the
// recovered events of its aggregate in the same FIFO. Handlers run, retry on
// failure and acknowledge exactly as for live events.
//
// Deliveries whose event no longer exists are acknowledged and skipped. An
// aggregate whose event cannot be rebuilt (a corrupt record, an upcaster error)
// is parked with the error reported to the publish error handler, and the other
// aggregates are recovered regardless; ResumeDeliveries retries it. Delivery is
// at-least-once: an event whose handlers finished but whose ack was lost to a
// crash is delivered again.
func (p *Processor[T]) Recover(
	ctx context.Context,
	batch int,
) error {
	if p.dispatcher == nil {
		return nil
	}

	if batch <= 0 {
		batch = 100
	}

	backlogs, order, err := p.pendingByAggregate(
		ctx,
		batch,
		func(
			string,
		) bool {
			return true
		},
	)
	if err != nil {
		return err
	}

	return utils.FirstError(
		order,
		func(
			agg string,
		) error {
			return p.recoverAggregate(
				ctx,
				agg,
				backlogs[agg],
			)
		},
	)
}

func (p *Processor[T]) recoverAggregate(
	ctx context.Context,
	agg string,
	backlog []asynxmd.Delivery,
) error {
	undelivered, err := p.redeliver(
		ctx,
		agg,
		backlog,
	)
	if err == nil {
		return nil
	}

	if recoveryMustStop(
		ctx,
		err,
	) {
		return fmt.Errorf(
			"asynx: recovery: %w",
			err,
		)
	}

	p.dispatcher.Park(
		agg,
		err,
		undelivered...,
	)
	p.reportPublishError(
		ctx,
		asynxmd.Event[T]{AggregateID: agg, Version: backlog[0].Version},
		fmt.Errorf(
			"recovery: %w",
			err,
		),
	)
	return nil
}

func recoveryMustStop(
	ctx context.Context,
	err error,
) bool {
	if ctx.Err() != nil {
		return true
	}

	return errors.Is(
		err,
		asynxmd.ErrDispatcherClosed,
	)
}

func (p *Processor[T]) scanPending(
	ctx context.Context,
	batch int,
	visit func(asynxmd.Delivery) error,
) error {
	return p.scanFrom(
		ctx,
		batch,
		make(map[asynxmd.Delivery]struct{}),
		visit,
	)
}

func (p *Processor[T]) scanFrom(
	ctx context.Context,
	limit int,
	seen map[asynxmd.Delivery]struct{},
	visit func(asynxmd.Delivery) error,
) error {
	pending, err := p.es.Store().PendingDeliveries(
		ctx,
		limit,
	)
	if err != nil {
		return fmt.Errorf(
			"asynx: recovery: %w",
			err,
		)
	}

	err = visitUnseen(
		pending,
		seen,
		visit,
	)
	if err != nil {
		return fmt.Errorf(
			"asynx: recovery: %w",
			err,
		)
	}

	if len(pending) < limit {
		return nil
	}

	return p.scanFrom(
		ctx,
		limit*2,
		seen,
		visit,
	)
}

func visitUnseen(
	pending []asynxmd.Delivery,
	seen map[asynxmd.Delivery]struct{},
	visit func(asynxmd.Delivery) error,
) error {
	unseen := slices.DeleteFunc(
		slices.Clone(pending),
		func(
			d asynxmd.Delivery,
		) bool {
			_, again := seen[d]
			seen[d] = struct{}{}
			return again
		},
	)
	return utils.FirstError(
		unseen,
		visit,
	)
}

func (p *Processor[T]) pendingByAggregate(
	ctx context.Context,
	batch int,
	want func(aggregateID string) bool,
) (backlogs map[string][]asynxmd.Delivery, order []string, err error) {
	backlogs = make(map[string][]asynxmd.Delivery)
	err = p.scanPending(
		ctx,
		batch,
		func(
			d asynxmd.Delivery,
		) error {
			if !want(d.AggregateID) {
				return nil
			}

			if _, known := backlogs[d.AggregateID]; !known {
				order = append(
					order,
					d.AggregateID,
				)
			}

			backlogs[d.AggregateID] = append(
				backlogs[d.AggregateID],
				d,
			)
			return nil
		},
	)
	return backlogs,
		order,
		err
}

func (p *Processor[T]) redeliver(
	ctx context.Context,
	agg string,
	backlog []asynxmd.Delivery,
) (notEnqueued []int64, err error) {
	if len(backlog) == 0 {
		return nil,
			nil
	}

	vanished, err := p.enqueueBacklog(
		ctx,
		agg,
		backlog,
	)
	if err != nil {
		return slices.Sorted(maps.Keys(vanished)),
			err
	}

	return nil,
		p.ackVanished(
			ctx,
			agg,
			backlog,
			vanished,
		)
}

func (p *Processor[T]) enqueueBacklog(
	ctx context.Context,
	agg string,
	backlog []asynxmd.Delivery,
) (vanished map[int64]struct{}, err error) {
	vanished = make(
		map[int64]struct{},
		len(backlog),
	)
	lowest, highest := backlog[0].Version, backlog[0].Version
	for _, d := range backlog {
		vanished[d.Version] = struct{}{}
		lowest, highest = min(
			lowest,
			d.Version,
		), max(
			highest,
			d.Version,
		)
	}

	enqueuer := &backlogEnqueuer[T]{
		dispatcher: p.dispatcher,
		store:      p.es.Store(),
		aggregate:  agg,
		vanished:   vanished,
	}
	replayErr := p.es.Replay(
		ctx,
		agg,
		lowest,
		highest,
		func(
			_ context.Context,
			evt asynxmd.Event[T],
		) {
			enqueuer.enqueue(
				ctx,
				evt,
			)
		},
	)
	return vanished,
		errors.Join(
			replayErr,
			enqueuer.err,
		)
}

func (p *Processor[T]) ackVanished(
	ctx context.Context,
	agg string,
	backlog []asynxmd.Delivery,
	vanished map[int64]struct{},
) error {
	return utils.FirstError(
		backlog,
		func(
			d asynxmd.Delivery,
		) error {
			if _, isVanished := vanished[d.Version]; !isVanished {
				return nil
			}

			return p.es.Store().AckDelivery(
				ctx,
				agg,
				d.Version,
			)
		},
	)
}

package processor

import (
	"context"
	"errors"
	"fmt"
	"slices"

	asynxmd "github.com/char2cs/asynx/models"
)

// Paused lists the aggregates whose durable delivery is parked: a delivery gave
// up (see ErrDeliveryParked), or a Forget could not deliver its marker.
func (p *Processor[T]) Paused() []string {
	if p.dispatcher == nil {
		return nil
	}
	return p.dispatcher.Halted()
}

// Resume redelivers the parked aggregates, to the durable subscribers. For each
// it lifts the park and re-enqueues the aggregate's pending deliveries, in
// order, under the aggregate's command lock so no live event overtakes them.
// The pending deliveries include a deletion marker left by a failed Forget,
// which therefore goes out before anything that followed it. Call it once the
// cause (a bad handler, a downstream outage) is fixed.
//
// An aggregate that cannot be redelivered stays parked and the others are still
// resumed; the errors are joined.
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
	var errs []error
	halted := slices.Sorted(slices.Values(p.dispatcher.Halted()))
	for group := range slices.Chunk(halted, resumeGroupSize) {
		err := p.withLocks(ctx, group, func() error {
			return p.resumeGroup(ctx, batch, group)
		})
		if err == nil {
			continue
		}
		errs = append(errs, err)
		if ctx.Err() != nil {
			break
		}
	}
	return errors.Join(errs...)
}

const resumeGroupSize = 64

// withLocks runs fn holding the command locks of aggs, taken in the order given.
// Callers pass them sorted, so concurrent callers cannot deadlock.
func (p *Processor[T]) withLocks(
	ctx context.Context,
	aggs []string,
	fn func() error,
) error {
	if len(aggs) == 0 {
		return fn()
	}
	return p.executor.WithLock(ctx, aggs[0], func() error {
		return p.withLocks(ctx, aggs[1:], fn)
	})
}

// resumeGroup lifts the park of each aggregate in group that is still parked and
// redelivers their pending entries, reading the delivery log once for the whole
// group. An aggregate that cannot be redelivered is parked again, because a
// lifted park would hide that from the next Resume; the others are still
// resumed. The caller holds the command lock of every aggregate in group, so no
// live event overtakes the redelivery and no entry appears behind the scan.
func (p *Processor[T]) resumeGroup(
	ctx context.Context,
	batch int,
	group []string,
) error {
	stillParked := make(map[string]struct{}, len(group))
	for _, agg := range group {
		if p.dispatcher.Resume(agg) {
			stillParked[agg] = struct{}{}
		}
	}
	if len(stillParked) == 0 {
		return nil
	}

	backlogs, order, err := p.pendingByAggregate(ctx, batch, func(id string) bool {
		_, wanted := stillParked[id]
		return wanted
	})
	if err != nil {
		p.parkAll(stillParked)
		return err
	}

	var errs []error
	for _, agg := range order {
		if err := p.redeliver(ctx, agg, backlogs[agg]); err != nil {
			p.dispatcher.Park(agg)
			errs = append(errs, fmt.Errorf("asynx: resume %s: %w", agg, err))
		}
	}
	return errors.Join(errs...)
}

func (p *Processor[T]) parkAll(aggs map[string]struct{}) {
	for agg := range aggs {
		p.dispatcher.Park(agg)
	}
}

// Recover re-enqueues every unacknowledged delivery, in enqueue order, on the
// per-aggregate dispatcher, for the durable subscribers (live subscribers never
// see what happened while the process was down). It returns once they are all
// queued, not once they are handled: a handler that keeps failing must not stop
// the process from starting. Ordering against live commands is still
// guaranteed, because a live event enqueues behind the recovered events of its
// aggregate in the same FIFO. Handlers run, retry on failure and acknowledge
// exactly as for live events.
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
	backlogs, order, err := p.pendingByAggregate(ctx, batch, func(string) bool { return true })
	if err != nil {
		return err
	}
	for _, agg := range order {
		err := p.redeliver(ctx, agg, backlogs[agg])
		if err == nil {
			continue
		}
		if ctx.Err() != nil || errors.Is(err, asynxmd.ErrDispatcherClosed) {
			return fmt.Errorf("asynx: recovery: %w", err)
		}
		p.dispatcher.Park(agg)
		p.dispatcher.ReportError(ctx, asynxmd.Event[T]{AggregateID: agg, Version: backlogs[agg][0].Version},
			fmt.Errorf("recovery: %w", err))
	}
	return nil
}

// scanPending calls visit for every pending delivery once, in enqueue order. It
// reads the log in growing prefixes, so the total work stays linear. Entries
// are acknowledged while it runs, which shifts the prefix, so what has been
// visited is tracked by key rather than by position.
func (p *Processor[T]) scanPending(
	ctx context.Context,
	batch int,
	visit func(asynxmd.Delivery) error,
) error {
	store := p.es.Store()
	seen := make(map[asynxmd.Delivery]struct{})
	for limit := batch; ; limit *= 2 {
		pending, err := store.PendingDeliveries(ctx, limit)
		if err != nil {
			return fmt.Errorf("asynx: recovery: %w", err)
		}
		if err := visitUnseen(pending, seen, visit); err != nil {
			return fmt.Errorf("asynx: recovery: %w", err)
		}
		if len(pending) < limit {
			return nil
		}
	}
}

func visitUnseen(
	pending []asynxmd.Delivery,
	seen map[asynxmd.Delivery]struct{},
	visit func(asynxmd.Delivery) error,
) error {
	for _, d := range pending {
		if _, again := seen[d]; again {
			continue
		}
		seen[d] = struct{}{}
		if err := visit(d); err != nil {
			return err
		}
	}
	return nil
}

// pendingByAggregate reads the delivery log and groups the entries of the
// aggregates that want selects by aggregate, in enqueue order. order lists the
// aggregates by first appearance.
func (p *Processor[T]) pendingByAggregate(
	ctx context.Context,
	batch int,
	want func(aggregateID string) bool,
) (backlogs map[string][]asynxmd.Delivery, order []string, err error) {
	backlogs = make(map[string][]asynxmd.Delivery)
	err = p.scanPending(ctx, batch, func(d asynxmd.Delivery) error {
		if !want(d.AggregateID) {
			return nil
		}
		if _, known := backlogs[d.AggregateID]; !known {
			order = append(order, d.AggregateID)
		}
		backlogs[d.AggregateID] = append(backlogs[d.AggregateID], d)
		return nil
	})
	return backlogs, order, err
}

// redeliver rebuilds the events behind one aggregate's pending deliveries and
// enqueues them in version order. The stream is replayed once, however many
// entries are pending, so a long backlog costs one pass over the stream rather
// than one per entry. Entries whose event no longer exists are acknowledged
// instead. On an error the entries not yet enqueued stay pending.
func (p *Processor[T]) redeliver(
	ctx context.Context,
	agg string,
	backlog []asynxmd.Delivery,
) error {
	if len(backlog) == 0 {
		return nil
	}
	vanished, err := p.enqueueBacklog(ctx, agg, backlog)
	if err != nil {
		return err
	}
	return p.ackVanished(ctx, agg, backlog, vanished)
}

// enqueueBacklog replays the stream once and enqueues the events the backlog
// names. It returns the versions whose event was not found.
func (p *Processor[T]) enqueueBacklog(
	ctx context.Context,
	agg string,
	backlog []asynxmd.Delivery,
) (vanished map[int64]struct{}, err error) {
	store := p.es.Store()
	ack := func(version int64) func(context.Context) error {
		return func(ctx context.Context) error { return store.AckDelivery(ctx, agg, version) }
	}

	vanished = make(map[int64]struct{}, len(backlog))
	lowest, highest := backlog[0].Version, backlog[0].Version
	for _, d := range backlog {
		vanished[d.Version] = struct{}{}
		lowest, highest = min(lowest, d.Version), max(highest, d.Version)
	}

	var enqueueErr error
	replayErr := p.es.Replay(ctx, agg, lowest, highest,
		func(_ context.Context, evt asynxmd.Event[T]) {
			if _, wanted := vanished[evt.Version]; !wanted || enqueueErr != nil {
				return
			}
			delete(vanished, evt.Version)
			_, enqueueErr = p.dispatcher.EnqueueRedelivery(ctx, evt, ack(evt.Version))
		})
	return vanished, errors.Join(replayErr, enqueueErr)
}

func (p *Processor[T]) ackVanished(
	ctx context.Context,
	agg string,
	backlog []asynxmd.Delivery,
	vanished map[int64]struct{},
) error {
	for _, d := range backlog {
		if _, isVanished := vanished[d.Version]; !isVanished {
			continue
		}
		if err := p.es.Store().AckDelivery(ctx, agg, d.Version); err != nil {
			return err
		}
	}
	return nil
}

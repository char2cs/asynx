package processor

import (
	"context"
	"errors"
	"fmt"

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
	for _, agg := range p.dispatcher.Halted() {
		err := p.executor.WithLock(ctx, agg, func() error {
			return p.resumeAggregate(ctx, batch, agg)
		})
		if err == nil {
			continue
		}
		errs = append(errs, fmt.Errorf("asynx: resume %s: %w", agg, err))
		if ctx.Err() != nil {
			break
		}
	}
	return errors.Join(errs...)
}

// resumeAggregate lifts the aggregate's park and redelivers its pending
// entries, leaving it parked when they cannot all be queued: a lifted park would
// hide that from the next Resume. It does nothing if a concurrent Resume lifted
// the park first. The caller holds the aggregate's command lock.
func (p *Processor[T]) resumeAggregate(
	ctx context.Context,
	batch int,
	agg string,
) error {
	wasParked := p.dispatcher.Resume(agg)
	if !wasParked {
		return nil
	}
	backlogs, _, err := p.pendingByAggregate(ctx, batch, func(id string) bool { return id == agg })
	if err == nil {
		err = p.redeliver(ctx, agg, backlogs[agg])
	}
	if err != nil {
		p.dispatcher.Park(agg)
	}
	return err
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
		for _, d := range pending {
			if _, again := seen[d]; again {
				continue
			}
			seen[d] = struct{}{}
			if err := visit(d); err != nil {
				return fmt.Errorf("asynx: recovery: %w", err)
			}
		}
		if len(pending) < limit {
			return nil
		}
	}
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
	store := p.es.Store()
	ack := func(version int64) func(context.Context) error {
		return func(ctx context.Context) error { return store.AckDelivery(ctx, agg, version) }
	}

	pending := make(map[int64]struct{}, len(backlog))
	lowest, highest := backlog[0].Version, backlog[0].Version
	for _, d := range backlog {
		pending[d.Version] = struct{}{}
		lowest, highest = min(lowest, d.Version), max(highest, d.Version)
	}

	var enqueueErr error
	err := p.es.Replay(ctx, agg, lowest, highest,
		func(_ context.Context, evt asynxmd.Event[T]) {
			if _, wanted := pending[evt.Version]; !wanted || enqueueErr != nil {
				return
			}
			delete(pending, evt.Version)
			_, enqueueErr = p.dispatcher.EnqueueRedelivery(ctx, evt, ack(evt.Version))
		})
	if err = errors.Join(err, enqueueErr); err != nil {
		return err
	}

	for _, d := range backlog {
		if _, vanished := pending[d.Version]; vanished {
			if err := store.AckDelivery(ctx, agg, d.Version); err != nil {
				return err
			}
		}
	}
	return nil
}

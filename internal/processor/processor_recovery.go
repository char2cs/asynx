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
func (p *Processor[T]) Resume(ctx context.Context, batch int) error {
	if p.dispatcher == nil {
		return nil
	}
	if batch <= 0 {
		batch = 100
	}
	var errs []error
	for _, agg := range p.dispatcher.Halted() {
		err := p.executor.WithLock(ctx, agg, func() error {
			p.dispatcher.Resume(agg)
			err := p.scanPending(ctx, batch, func(d asynxmd.Delivery) error {
				if d.AggregateID != agg {
					return nil
				}
				return p.redeliver(ctx, d)
			})
			if err != nil {
				// Leave it parked: its entries are not all queued, and a
				// lifted park would hide that from the next Resume.
				p.dispatcher.Park(agg)
			}
			return err
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("asynx: resume %s: %w", agg, err))
			if ctx.Err() != nil {
				break
			}
		}
	}
	return errors.Join(errs...)
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
func (p *Processor[T]) Recover(ctx context.Context, batch int) error {
	if p.dispatcher == nil {
		return nil
	}
	if batch <= 0 {
		batch = 100
	}
	failed := make(map[string]struct{})
	return p.scanPending(ctx, batch, func(d asynxmd.Delivery) error {
		if _, skip := failed[d.AggregateID]; skip {
			return nil // behind a failed event of its aggregate: stays pending
		}
		err := p.redeliver(ctx, d)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil || errors.Is(err, asynxmd.ErrDispatcherClosed) {
			return err
		}
		failed[d.AggregateID] = struct{}{}
		p.dispatcher.Park(d.AggregateID)
		p.dispatcher.ReportError(ctx, asynxmd.Event[T]{AggregateID: d.AggregateID, Version: d.Version},
			fmt.Errorf("recovery: %w", err))
		return nil
	})
}

// scanPending calls visit for every pending delivery once, in enqueue order. It
// reads the log in growing prefixes, so the total work stays linear. Entries
// are acknowledged while it runs, which shifts the prefix, so what has been
// visited is tracked by key rather than by position.
func (p *Processor[T]) scanPending(ctx context.Context, batch int, visit func(asynxmd.Delivery) error) error {
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

// redeliver rebuilds the event behind d and enqueues it. When the event no
// longer exists the delivery is acknowledged instead.
func (p *Processor[T]) redeliver(ctx context.Context, d asynxmd.Delivery) error {
	store := p.es.Store()

	var event asynxmd.Event[T]
	found := false
	err := p.es.Replay(ctx, d.AggregateID, d.Version, d.Version,
		func(_ context.Context, evt asynxmd.Event[T]) {
			if evt.Version == d.Version {
				event, found = evt, true
			}
		})
	if err != nil {
		return err
	}

	if !found {
		return store.AckDelivery(ctx, d.AggregateID, d.Version)
	}

	_, err = p.dispatcher.EnqueueRedelivery(ctx, event, func(ctx context.Context) error {
		return store.AckDelivery(ctx, d.AggregateID, d.Version)
	})
	return err
}

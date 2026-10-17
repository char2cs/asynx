package processor

import (
	"context"
	"fmt"

	asynxmd "github.com/char2cs/asynx/models"
)

// Resume redelivers the aggregates parked after a delivery gave up (see
// ErrDeliveryParked) or overflowed its queue. For each it lifts the park and
// re-enqueues the aggregate's pending deliveries, in order, under the
// aggregate's command lock so no live event overtakes them. Call it once the
// cause (a bad handler, a downstream outage) is fixed.
func (p *Processor[T]) Resume(ctx context.Context, batch int) error {
	if p.dispatcher == nil {
		return nil
	}
	if batch <= 0 {
		batch = 100
	}
	for _, agg := range p.dispatcher.Halted() {
		err := p.executor.WithLock(ctx, agg, func() error {
			p.dispatcher.Resume(agg)
			if err := p.redeliverPending(ctx, batch, agg); err != nil {
				// Leave it parked: its entries are not all queued, and a
				// lifted park would hide that from the next Resume.
				p.dispatcher.Park(agg)
				return err
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("asynx: resume %s: %w", agg, err)
		}
	}
	return nil
}

// Recover re-enqueues every unacknowledged delivery, in enqueue order, on the
// per-aggregate dispatcher. It returns once they are all queued, not once they
// are handled: a handler that keeps failing must not stop the process from
// starting. Ordering against live commands is still guaranteed, because a live
// event enqueues behind the recovered events of its aggregate in the same FIFO.
// Handlers run, retry on failure and acknowledge exactly as for live events.
//
// Deliveries whose event no longer exists are acknowledged and skipped.
// Delivery is at-least-once: an event whose handlers finished but whose ack was
// lost to a crash is delivered again.
func (p *Processor[T]) Recover(ctx context.Context, batch int) error {
	if p.dispatcher == nil {
		return nil
	}
	if batch <= 0 {
		batch = 100
	}
	return p.redeliverPending(ctx, batch, "")
}

// redeliverPending re-enqueues pending deliveries in enqueue order: all of
// them, or only those of aggregateID when it is not empty.
func (p *Processor[T]) redeliverPending(ctx context.Context, batch int, aggregateID string) error {
	store := p.es.Store()
	seen := make(map[asynxmd.Delivery]struct{})

	for {
		// Entries already seen sit at the front (the log is in enqueue order),
		// so asking for that many plus a batch always reaches new ones.
		pending, err := store.PendingDeliveries(ctx, len(seen)+batch)
		if err != nil {
			return fmt.Errorf("asynx: recovery: %w", err)
		}

		fresh := 0
		for _, d := range pending {
			if _, again := seen[d]; again {
				continue
			}
			seen[d] = struct{}{}
			fresh++

			if aggregateID != "" && d.AggregateID != aggregateID {
				continue
			}
			if err := p.redeliver(ctx, d); err != nil {
				return fmt.Errorf("asynx: recovery: %w", err)
			}
		}
		if fresh == 0 {
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

	_, err = p.dispatcher.EnqueueRecovered(ctx, event, func(ctx context.Context) error {
		return store.AckDelivery(ctx, d.AggregateID, d.Version)
	})
	return err
}

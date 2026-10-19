package dispatchertest

import (
	"context"
	"sync"

	"github.com/char2cs/asynx/models"
)

type reorderingDispatcher struct {
	models.Dispatcher[Aggregate]
	mu   sync.Mutex
	held map[string]heldRequest
}

func reorder(
	inner models.Dispatcher[Aggregate],
) models.Dispatcher[Aggregate] {
	return &reorderingDispatcher{Dispatcher: inner, held: map[string]heldRequest{}}
}

func (d *reorderingDispatcher) Enqueue(
	ctx context.Context,
	r models.DispatchRequest[Aggregate],
) (models.Ticket, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	id := r.Event.AggregateID
	held, isHeld := d.held[id]
	if !isHeld {
		d.held[id] = heldRequest{ctx: ctx, request: r, tickets: make(chan models.Ticket, 1)}
		return d.held[id].ticket(), nil
	}
	delete(d.held, id)
	tk, err := d.Dispatcher.Enqueue(ctx, r)
	held.release(d.Dispatcher)
	return tk, err
}

func (d *reorderingDispatcher) flush() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, held := range d.held {
		held.release(d.Dispatcher)
	}
	clear(d.held)
}

func (d *reorderingDispatcher) WaitIdle() {
	d.flush()
	d.Dispatcher.WaitIdle()
}

func (d *reorderingDispatcher) Close(
	ctx context.Context,
) error {
	d.flush()
	return d.Dispatcher.Close(ctx)
}

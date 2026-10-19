package dispatchertest

import (
	"context"
	"sync/atomic"

	"github.com/char2cs/asynx/models"
)

type heldRequest struct {
	ctx     context.Context
	request models.DispatchRequest[Aggregate]
	tickets chan models.Ticket
}

func (h heldRequest) ticket() models.Ticket {
	settled := make(chan struct{})
	final := make(chan struct{})
	var inner atomic.Pointer[models.Ticket]
	go relay(h.tickets, &inner, settled, final)
	return models.Ticket{
		Settled: settled,
		Final:   final,
		Err:     func() error { return inner.Load().Err() },
		Unrun:   func() error { return inner.Load().Unrun() },
	}
}

func (h heldRequest) release(
	d models.Dispatcher[Aggregate],
) {
	tk, err := d.Enqueue(h.ctx, h.request)
	if err != nil {
		return
	}
	h.tickets <- tk
}

func relay(
	tickets <-chan models.Ticket,
	inner *atomic.Pointer[models.Ticket],
	settled chan<- struct{},
	final chan<- struct{},
) {
	tk := <-tickets
	inner.Store(&tk)
	<-tk.Settled
	close(settled)
	<-tk.Final
	close(final)
}

package projections

import (
	"context"
	"fmt"
	"github.com/char2cs/asynx/example/domain"
	"github.com/char2cs/asynx/models"
	"slices"
	"sync"
)

// OrderNotifier is the example's notification projection: it turns each order
// event into a human-readable message and keeps the messages in an outbox, in
// the order the events were handled, standing in for the emails and warehouse
// calls a real system would make. The zero value is ready to use and it is safe
// for concurrent use.
type OrderNotifier struct {
	mu     sync.Mutex
	outbox []string
}

// Handle is a models.ProjectionHandler for order events. It appends one
// notification to the outbox for OrderCreated, OrderConfirmed and OrderShipped,
// naming the order, and ignores any other event. It cannot fail and ctx is not
// used.
func (n *OrderNotifier) Handle(
	ctx context.Context,
	event models.Event[domain.Order],
) {
	n.mu.Lock()
	defer n.mu.Unlock()

	switch event.EventName {
	case "OrderCreated":
		n.outbox = append(
			n.outbox,
			fmt.Sprintf(
				"📧 Confirmation email sent for order %s",
				event.Aggregate.ID,
			),
		)
	case "OrderConfirmed":
		n.outbox = append(
			n.outbox,
			fmt.Sprintf(
				"🎉 Order %s confirmed! Warehouse notified.",
				event.Aggregate.ID,
			),
		)
	case "OrderShipped":
		n.outbox = append(
			n.outbox,
			fmt.Sprintf(
				"📦 Order %s shipped! Customer notified.",
				event.Aggregate.ID,
			),
		)
	}
}

// GetOutbox returns a copy of the notifications recorded so far, oldest first.
// The copy is the caller's: later events do not change it.
func (n *OrderNotifier) GetOutbox() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return slices.Clone(n.outbox)
}

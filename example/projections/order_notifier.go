package projections

import (
	"context"
	"fmt"
	"github.com/char2cs/asynx/example/domain"
	"github.com/char2cs/asynx/models"
	"sync"
)

// OrderNotifier maintains an outbox of notifications.
type OrderNotifier struct {
	mu     sync.Mutex
	outbox []string
}

// Handle records a notification for the event.
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

// GetOutbox returns a copy of the notifications recorded so far.
func (n *OrderNotifier) GetOutbox() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.outbox
}

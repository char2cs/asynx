package projections

import (
	"context"
	"fmt"
	"sync"

	"github.com/char2cs/asynx/example/domain"
	"github.com/char2cs/asynx/models"
)

// OrderReporter prints order events to console.
type OrderReporter struct {
	mu sync.Mutex
}

func (r *OrderReporter) Handle(ctx context.Context, event models.Event[domain.Order]) {
	r.mu.Lock()
	defer r.mu.Unlock()

	fmt.Printf("[EVENT] %s - Order %s: %s → %s\n",
		event.EventName,
		event.Aggregate.ID,
		event.PreviousAggregate.Status,
		event.Aggregate.Status,
	)
}

// OrderNotifier maintains an outbox of notifications.
type OrderNotifier struct {
	mu     sync.Mutex
	outbox []string
}

func (n *OrderNotifier) Handle(ctx context.Context, event models.Event[domain.Order]) {
	n.mu.Lock()
	defer n.mu.Unlock()

	switch event.EventName {
	case "OrderCreated":
		n.outbox = append(n.outbox, fmt.Sprintf(
			"📧 Confirmation email sent for order %s",
			event.Aggregate.ID,
		))
	case "OrderConfirmed":
		n.outbox = append(n.outbox, fmt.Sprintf(
			"🎉 Order %s confirmed! Warehouse notified.",
			event.Aggregate.ID,
		))
	case "OrderShipped":
		n.outbox = append(n.outbox, fmt.Sprintf(
			"📦 Order %s shipped! Customer notified.",
			event.Aggregate.ID,
		))
	}
}

func (n *OrderNotifier) GetOutbox() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.outbox
}

package projections

import (
	"context"
	"fmt"
	"github.com/char2cs/asynx/example/domain"
	"github.com/char2cs/asynx/models"
	"sync"
)

// OrderReporter prints order events to console.
type OrderReporter struct {
	mu sync.Mutex
}

func (r *OrderReporter) Handle(
	ctx context.Context,
	event models.Event[domain.Order],
) {
	r.mu.Lock()
	defer r.mu.Unlock()

	fmt.Printf(
		"[EVENT] %s - Order %s: %s → %s\n",
		event.EventName,
		event.Aggregate.ID,
		event.PreviousAggregate.Status,
		event.Aggregate.Status,
	)
}

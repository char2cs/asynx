package projections

import (
	"context"
	"fmt"
	"github.com/char2cs/asynx/example/domain"
	"github.com/char2cs/asynx/models"
	"sync"
)

// OrderReporter is the example's logging projection: it prints one line per
// order event to standard output, showing the event name, the order ID and the
// status change. The zero value is ready to use and it is safe for concurrent
// use; the lock keeps lines from concurrent events from interleaving.
type OrderReporter struct {
	mu sync.Mutex
}

// Handle is a models.ProjectionHandler for order events. It prints
// "[EVENT] <name> - Order <id>: <previous status> → <new status>" for the
// event. It cannot fail and ctx is not used.
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

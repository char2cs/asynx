package projections

import (
	"context"
	"sync"
	"testing"

	"github.com/char2cs/asynx/example/domain"
	"github.com/char2cs/asynx/models"
)

// ============================================================================
// OrderReporter Tests
// ============================================================================

func TestOrderReporter_Handle_OrderCreated(t *testing.T) {
	reporter := &OrderReporter{}
	ctx := context.Background()
	event := models.Event[domain.Order]{
		EventName: "OrderCreated",
		Aggregate: domain.Order{
			ID:     "ORD-001",
			Status: domain.OrderPending,
			Total:  99.99,
			Items:  2,
			ShipTo: "123 Main St",
		},
		PreviousAggregate: domain.Order{},
	}

	// Should not panic
	reporter.Handle(ctx, event)
}

func TestOrderReporter_Handle_OrderConfirmed(t *testing.T) {
	reporter := &OrderReporter{}
	ctx := context.Background()
	event := models.Event[domain.Order]{
		EventName: "OrderConfirmed",
		Aggregate: domain.Order{
			ID:     "ORD-001",
			Status: domain.OrderConfirmed,
			Total:  99.99,
			Items:  2,
			ShipTo: "123 Main St",
		},
		PreviousAggregate: domain.Order{
			ID:     "ORD-001",
			Status: domain.OrderPending,
			Total:  99.99,
			Items:  2,
			ShipTo: "123 Main St",
		},
	}

	reporter.Handle(ctx, event)
}

func TestOrderReporter_Handle_OrderShipped(t *testing.T) {
	reporter := &OrderReporter{}
	ctx := context.Background()
	event := models.Event[domain.Order]{
		EventName: "OrderShipped",
		Aggregate: domain.Order{
			ID:     "ORD-001",
			Status: domain.OrderShipped,
			Total:  99.99,
			Items:  2,
			ShipTo: "123 Main St",
		},
		PreviousAggregate: domain.Order{
			ID:     "ORD-001",
			Status: domain.OrderConfirmed,
			Total:  99.99,
			Items:  2,
			ShipTo: "123 Main St",
		},
	}

	reporter.Handle(ctx, event)
}

func TestOrderReporter_Handle_Concurrent(t *testing.T) {
	reporter := &OrderReporter{}
	ctx := context.Background()
	wg := sync.WaitGroup{}

	// Simulate concurrent event handling
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			event := models.Event[domain.Order]{
				EventName: "OrderCreated",
				Aggregate: domain.Order{
					ID:     "ORD-001",
					Status: domain.OrderPending,
				},
			}
			reporter.Handle(ctx, event)
		}(i)
	}

	wg.Wait()
}

// ============================================================================
// OrderNotifier Tests
// ============================================================================

func TestOrderNotifier_Handle_OrderCreated(t *testing.T) {
	notifier := &OrderNotifier{}
	ctx := context.Background()
	event := models.Event[domain.Order]{
		EventName: "OrderCreated",
		Aggregate: domain.Order{
			ID:     "ORD-001",
			Status: domain.OrderPending,
			Total:  99.99,
			Items:  2,
			ShipTo: "123 Main St",
		},
	}

	notifier.Handle(ctx, event)
	outbox := notifier.GetOutbox()

	if len(outbox) != 1 {
		t.Errorf("GetOutbox() length = %d, want 1", len(outbox))
	}
	if outbox[0] != "📧 Confirmation email sent for order ORD-001" {
		t.Errorf("GetOutbox()[0] = %q, want %q", outbox[0], "📧 Confirmation email sent for order ORD-001")
	}
}

func TestOrderNotifier_Handle_OrderConfirmed(t *testing.T) {
	notifier := &OrderNotifier{}
	ctx := context.Background()
	event := models.Event[domain.Order]{
		EventName: "OrderConfirmed",
		Aggregate: domain.Order{
			ID:     "ORD-002",
			Status: domain.OrderConfirmed,
			Total:  149.99,
			Items:  3,
			ShipTo: "456 Oak Ave",
		},
	}

	notifier.Handle(ctx, event)
	outbox := notifier.GetOutbox()

	if len(outbox) != 1 {
		t.Errorf("GetOutbox() length = %d, want 1", len(outbox))
	}
	if outbox[0] != "🎉 Order ORD-002 confirmed! Warehouse notified." {
		t.Errorf("GetOutbox()[0] = %q, want %q", outbox[0], "🎉 Order ORD-002 confirmed! Warehouse notified.")
	}
}

func TestOrderNotifier_Handle_OrderShipped(t *testing.T) {
	notifier := &OrderNotifier{}
	ctx := context.Background()
	event := models.Event[domain.Order]{
		EventName: "OrderShipped",
		Aggregate: domain.Order{
			ID:     "ORD-003",
			Status: domain.OrderShipped,
			Total:  199.99,
			Items:  5,
			ShipTo: "789 Elm St",
		},
	}

	notifier.Handle(ctx, event)
	outbox := notifier.GetOutbox()

	if len(outbox) != 1 {
		t.Errorf("GetOutbox() length = %d, want 1", len(outbox))
	}
	if outbox[0] != "📦 Order ORD-003 shipped! Customer notified." {
		t.Errorf("GetOutbox()[0] = %q, want %q", outbox[0], "📦 Order ORD-003 shipped! Customer notified.")
	}
}

func TestOrderNotifier_Handle_UnknownEvent(t *testing.T) {
	notifier := &OrderNotifier{}
	ctx := context.Background()
	event := models.Event[domain.Order]{
		EventName: "UnknownEvent",
		Aggregate: domain.Order{
			ID: "ORD-004",
		},
	}

	notifier.Handle(ctx, event)
	outbox := notifier.GetOutbox()

	if len(outbox) != 0 {
		t.Errorf("GetOutbox() length = %d, want 0", len(outbox))
	}
}

func TestOrderNotifier_Handle_MultipleEvents(t *testing.T) {
	notifier := &OrderNotifier{}
	ctx := context.Background()

	event1 := models.Event[domain.Order]{
		EventName: "OrderCreated",
		Aggregate: domain.Order{ID: "ORD-001"},
	}
	event2 := models.Event[domain.Order]{
		EventName: "OrderConfirmed",
		Aggregate: domain.Order{ID: "ORD-001"},
	}
	event3 := models.Event[domain.Order]{
		EventName: "OrderShipped",
		Aggregate: domain.Order{ID: "ORD-001"},
	}

	notifier.Handle(ctx, event1)
	notifier.Handle(ctx, event2)
	notifier.Handle(ctx, event3)

	outbox := notifier.GetOutbox()
	if len(outbox) != 3 {
		t.Errorf("GetOutbox() length = %d, want 3", len(outbox))
	}
}

func TestOrderNotifier_Handle_Concurrent(t *testing.T) {
	notifier := &OrderNotifier{}
	ctx := context.Background()
	wg := sync.WaitGroup{}

	// Simulate concurrent event handling
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			event := models.Event[domain.Order]{
				EventName: "OrderCreated",
				Aggregate: domain.Order{
					ID: "ORD-001",
				},
			}
			notifier.Handle(ctx, event)
		}(i)
	}

	wg.Wait()

	outbox := notifier.GetOutbox()
	if len(outbox) != 10 {
		t.Errorf("GetOutbox() length after concurrent calls = %d, want 10", len(outbox))
	}
}

func TestOrderNotifier_GetOutbox_EmptyInitially(t *testing.T) {
	notifier := &OrderNotifier{}
	outbox := notifier.GetOutbox()

	if len(outbox) != 0 {
		t.Errorf("GetOutbox() on fresh notifier = %d, want 0", len(outbox))
	}
}

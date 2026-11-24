package projections

import (
	"context"
	"github.com/char2cs/asynx/v2/example/domain"
	"github.com/char2cs/asynx/v2/models/event"
	"sync"
	"testing"
)

func TestOrderReporter_Handle_OrderCreated(
	t *testing.T,
) {
	reporter := &OrderReporter{}
	ctx := context.Background()
	evt := event.Event[domain.Order]{
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

	reporter.Handle(
		ctx,
		evt,
	)
}

func TestOrderReporter_Handle_OrderConfirmed(
	t *testing.T,
) {
	reporter := &OrderReporter{}
	ctx := context.Background()
	evt := event.Event[domain.Order]{
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

	reporter.Handle(
		ctx,
		evt,
	)
}

func TestOrderReporter_Handle_OrderShipped(
	t *testing.T,
) {
	reporter := &OrderReporter{}
	ctx := context.Background()
	evt := event.Event[domain.Order]{
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

	reporter.Handle(
		ctx,
		evt,
	)
}

func reportCreated(
	ctx context.Context,
	reporter *OrderReporter,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	reporter.Handle(
		ctx,
		event.Event[domain.Order]{
			EventName: "OrderCreated",
			Aggregate: domain.Order{
				ID:     "ORD-001",
				Status: domain.OrderPending,
			},
		},
	)
}

func TestOrderReporter_Handle_Concurrent(
	t *testing.T,
) {
	reporter := &OrderReporter{}
	ctx := context.Background()
	wg := sync.WaitGroup{}

	for range 10 {
		wg.Add(1)
		go reportCreated(
			ctx,
			reporter,
			&wg,
		)
	}

	wg.Wait()
}

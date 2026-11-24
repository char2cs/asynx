package projections

import (
	"context"
	"github.com/char2cs/asynx/example/domain"
	"github.com/char2cs/asynx/models"
	"sync"
	"testing"
)

func TestOrderReporter_Handle_OrderCreated(
	t *testing.T,
) {
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

	reporter.Handle(
		ctx,
		event,
	)
}

func TestOrderReporter_Handle_OrderConfirmed(
	t *testing.T,
) {
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

	reporter.Handle(
		ctx,
		event,
	)
}

func TestOrderReporter_Handle_OrderShipped(
	t *testing.T,
) {
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

	reporter.Handle(
		ctx,
		event,
	)
}

func TestOrderReporter_Handle_Concurrent(
	t *testing.T,
) {
	reporter := &OrderReporter{}
	ctx := context.Background()
	wg := sync.WaitGroup{}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(
			index int,
		) {
			defer wg.Done()
			event := models.Event[domain.Order]{
				EventName: "OrderCreated",
				Aggregate: domain.Order{
					ID:     "ORD-001",
					Status: domain.OrderPending,
				},
			}
			reporter.Handle(
				ctx,
				event,
			)
		}(i)
	}

	wg.Wait()
}

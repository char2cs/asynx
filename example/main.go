package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/char2cs/asynx"
	"github.com/char2cs/asynx/example/commands"
	"github.com/char2cs/asynx/example/domain"
	"github.com/char2cs/asynx/example/projections"
	"github.com/char2cs/asynx/internal/store"
)

func run(ctx context.Context) error {
	// ========================================================================
	// Setup: Create Asynx Instance using Public API
	// ========================================================================
	fmt.Println("=== Asynx E-Commerce Order Example ===")
	fmt.Println("Step 1: Building asynx instance...")

	// Create using the public asynx.Builder API
	// Use asynx's built-in in-memory store for this example
	memoryStore := store.New()

	ax, err := asynx.New[domain.Order]().
		WithEventStore(memoryStore).
		WithSnapshotStore(memoryStore).
		WithShardingOpts(asynx.ShardingOpts{
			Shards: 4,
		}).
		Build()
	if err != nil {
		return err
	}

	// ========================================================================
	// Setup: Register Event Subscribers
	// ========================================================================
	fmt.Println("Step 2: Registering event subscribers...")

	reporter := &projections.OrderReporter{}
	notifier := &projections.OrderNotifier{}

	// Subscribe to order events
	ax.Subscribe("Order.*", reporter.Handle) // Match: OrderCreated, OrderConfirmed, OrderShipped
	ax.Subscribe("Order.*", notifier.Handle)

	// ========================================================================
	// Execution: Submit Commands
	// ========================================================================
	fmt.Println("Step 3: Executing commands...")

	orders := []string{"ORD-001", "ORD-002", "ORD-003"}

	// Create orders
	for _, orderID := range orders {
		cmd := commands.CreateOrderCmd{
			ID:     orderID,
			Total:  99.99,
			Items:  2,
			ShipTo: "123 Main St",
		}

		if _, err := ax.Send(ctx, cmd); err != nil {
			fmt.Printf("❌ Failed to create order %s: %v\n", orderID, err)
			continue
		}
		fmt.Printf("✓ Order %s created\n", orderID)
	}
	ax.WaitPublish() // Wait for event handlers
	fmt.Println()

	// Confirm orders
	for _, orderID := range orders {
		cmd := commands.ConfirmOrderCmd{ID: orderID}
		if _, err := ax.Send(ctx, cmd); err != nil {
			fmt.Printf("❌ Failed to confirm order %s: %v\n", orderID, err)
			continue
		}
		fmt.Printf("✓ Order %s confirmed\n", orderID)
	}
	ax.WaitPublish() // Wait for event handlers
	fmt.Println()

	// Ship orders
	for _, orderID := range orders {
		cmd := commands.ShipOrderCmd{ID: orderID}
		if _, err := ax.Send(ctx, cmd); err != nil {
			fmt.Printf("❌ Failed to ship order %s: %v\n", orderID, err)
			continue
		}
		fmt.Printf("✓ Order %s shipped\n", orderID)
	}
	ax.WaitPublish() // Wait for event handlers
	fmt.Println()

	// ========================================================================
	// Verification: Query Aggregate State
	// ========================================================================
	fmt.Println("Step 4: Verifying final state...")

	for _, orderID := range orders {
		order, err := ax.Get(ctx, orderID)
		if err != nil {
			fmt.Printf("❌ Failed to load order %s: %v\n", orderID, err)
			continue
		}

		orderJSON, _ := json.MarshalIndent(order, "", "  ")
		fmt.Printf("Order %s:\n%s\n\n", orderID, string(orderJSON))
	}

	// ========================================================================
	// Output: Notification Outbox
	// ========================================================================
	fmt.Println("Step 5: Notifications sent:")
	for i, notification := range notifier.GetOutbox() {
		fmt.Printf("%d. %s\n", i+1, notification)
	}

	// ========================================================================
	// Cleanup: Graceful Shutdown
	// ========================================================================
	if err := ax.Shutdown(ctx); err != nil {
		fmt.Printf("❌ Shutdown error: %v\n", err)
		return err
	}

	fmt.Println("\n✅ Example completed successfully!")
	return nil
}

func main() {
	ctx := context.Background()
	if err := run(ctx); err != nil {
		panic(err)
	}
}

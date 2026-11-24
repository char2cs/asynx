package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/char2cs/asynx"
	"github.com/char2cs/asynx/example/commands"
	"github.com/char2cs/asynx/example/domain"
	"github.com/char2cs/asynx/example/projections"
	"github.com/char2cs/asynx/store"
)

func run(
	ctx context.Context,
) error {
	fmt.Println("=== Asynx E-Commerce Order Example ===")
	fmt.Println("Step 1: Building asynx instance...")

	eventStore := store.New()
	snapshotStore := store.NewSnapshots()

	ax, err := asynx.New[domain.Order]().
		WithEventStore(eventStore).
		WithSnapshotStore(snapshotStore).
		WithShardingOpts(asynx.ShardingOpts{
			Shards: 4,
		}).
		Build()
	if err != nil {
		return err
	}

	fmt.Println("Step 2: Registering event subscribers...")

	reporter := &projections.OrderReporter{}
	notifier := &projections.OrderNotifier{}

	ax.Subscribe(
		"Order.*",
		reporter.Handle,
	)
	ax.Subscribe(
		"Order.*",
		notifier.Handle,
	)

	fmt.Println("Step 3: Executing commands...")

	orders := []string{"ORD-001", "ORD-002", "ORD-003"}

	for _, orderID := range orders {
		cmd := commands.CreateOrderCmd{
			ID:     orderID,
			Total:  99.99,
			Items:  2,
			ShipTo: "123 Main St",
		}

		if _, err := ax.Send(
			ctx,
			cmd,
		); err != nil {
			fmt.Printf(
				"❌ Failed to create order %s: %v\n",
				orderID,
				err,
			)
			continue
		}

		fmt.Printf(
			"✓ Order %s created\n",
			orderID,
		)
	}

	ax.WaitPublish()
	fmt.Println()

	for _, orderID := range orders {
		cmd := commands.ConfirmOrderCmd{ID: orderID}
		if _, err := ax.Send(
			ctx,
			cmd,
		); err != nil {
			fmt.Printf(
				"❌ Failed to confirm order %s: %v\n",
				orderID,
				err,
			)
			continue
		}

		fmt.Printf(
			"✓ Order %s confirmed\n",
			orderID,
		)
	}

	ax.WaitPublish()
	fmt.Println()

	for _, orderID := range orders {
		cmd := commands.ShipOrderCmd{ID: orderID}
		if _, err := ax.Send(
			ctx,
			cmd,
		); err != nil {
			fmt.Printf(
				"❌ Failed to ship order %s: %v\n",
				orderID,
				err,
			)
			continue
		}

		fmt.Printf(
			"✓ Order %s shipped\n",
			orderID,
		)
	}

	ax.WaitPublish()
	fmt.Println()

	fmt.Println("Step 4: Verifying final state...")

	for _, orderID := range orders {
		order, err := ax.Get(
			ctx,
			orderID,
		)
		if err != nil {
			fmt.Printf(
				"❌ Failed to load order %s: %v\n",
				orderID,
				err,
			)
			continue
		}

		orderJSON, _ := json.MarshalIndent(
			order,
			"",
			"  ",
		)
		fmt.Printf(
			"Order %s:\n%s\n\n",
			orderID,
			string(orderJSON),
		)
	}

	fmt.Println("Step 5: Notifications sent:")
	for i, notification := range notifier.GetOutbox() {
		fmt.Printf(
			"%d. %s\n",
			i+1,
			notification,
		)
	}

	if err := ax.Shutdown(ctx); err != nil {
		fmt.Printf(
			"❌ Shutdown error: %v\n",
			err,
		)
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

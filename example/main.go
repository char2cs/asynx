package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/char2cs/asynx"
	"github.com/char2cs/asynx/example/commands"
	"github.com/char2cs/asynx/example/domain"
	"github.com/char2cs/asynx/example/projections"
	"github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
)

const (
	exampleShards = 4
	orderTotal    = 99.99
	orderItems    = 2
)

type phase struct {
	verb    string
	past    string
	factory func(orderID string) models.Command[domain.Order]
}

func build() (asynx.Asynx[domain.Order], error) {
	return asynx.New[domain.Order]().
		WithEventStore(store.New()).
		WithSnapshotStore(store.NewSnapshots()).
		WithShardingOpts(asynx.ShardingOpts{
			Shards: exampleShards,
		}).
		Build()
}

func subscribeAll(
	ax asynx.Asynx[domain.Order],
	reporter *projections.OrderReporter,
	notifier *projections.OrderNotifier,
) error {
	_, err := ax.Subscribe(
		"Order.*",
		reporter.Handle,
	)
	if err != nil {
		return err
	}

	_, err = ax.Subscribe(
		"Order.*",
		notifier.Handle,
	)

	return err
}

func sendOne(
	ctx context.Context,
	ax asynx.Asynx[domain.Order],
	orderID string,
	step phase,
) {
	_, err := ax.Send(
		ctx,
		step.factory(orderID),
	)
	if err != nil {
		fmt.Printf(
			"❌ Failed to %s order %s: %v\n",
			step.verb,
			orderID,
			err,
		)
		return
	}

	fmt.Printf(
		"✓ Order %s %s\n",
		orderID,
		step.past,
	)
}

func sendAll(
	ctx context.Context,
	ax asynx.Asynx[domain.Order],
	orders []string,
	step phase,
) {
	for _, orderID := range orders {
		sendOne(
			ctx,
			ax,
			orderID,
			step,
		)
	}

	ax.WaitPublish()
	fmt.Println()
}

func printOrder(
	ctx context.Context,
	ax asynx.Asynx[domain.Order],
	orderID string,
) error {
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
		return nil
	}

	orderJSON, err := json.MarshalIndent(
		order,
		"",
		"  ",
	)
	if err != nil {
		return err
	}

	fmt.Printf(
		"Order %s:\n%s\n\n",
		orderID,
		string(orderJSON),
	)

	return nil
}

func printOrders(
	ctx context.Context,
	ax asynx.Asynx[domain.Order],
	orders []string,
) error {
	if len(orders) == 0 {
		return nil
	}

	err := printOrder(
		ctx,
		ax,
		orders[0],
	)
	if err != nil {
		return err
	}

	return printOrders(
		ctx,
		ax,
		orders[1:],
	)
}

func createCmd(
	orderID string,
) models.Command[domain.Order] {
	return commands.CreateOrderCmd{
		ID:     orderID,
		Total:  orderTotal,
		Items:  orderItems,
		ShipTo: "123 Main St",
	}
}

func confirmCmd(
	orderID string,
) models.Command[domain.Order] {
	return commands.ConfirmOrderCmd{ID: orderID}
}

func shipCmd(
	orderID string,
) models.Command[domain.Order] {
	return commands.ShipOrderCmd{ID: orderID}
}

func sendPhases(
	ctx context.Context,
	ax asynx.Asynx[domain.Order],
	orders []string,
) {
	steps := []phase{
		{verb: "create", past: "created", factory: createCmd},
		{verb: "confirm", past: "confirmed", factory: confirmCmd},
		{verb: "ship", past: "shipped", factory: shipCmd},
	}
	for _, step := range steps {
		sendAll(
			ctx,
			ax,
			orders,
			step,
		)
	}
}

func printOutbox(
	notifier *projections.OrderNotifier,
) {
	fmt.Println("Step 5: Notifications sent:")
	for i, notification := range notifier.GetOutbox() {
		fmt.Printf(
			"%d. %s\n",
			i+1,
			notification,
		)
	}
}

func shutdown(
	ctx context.Context,
	ax asynx.Asynx[domain.Order],
) error {
	err := ax.Shutdown(ctx)
	if err != nil {
		fmt.Printf(
			"❌ Shutdown error: %v\n",
			err,
		)
		return err
	}

	fmt.Println("\n✅ Example completed successfully!")

	return nil
}

func setup() (asynx.Asynx[domain.Order], *projections.OrderNotifier, error) {
	fmt.Println("Step 1: Building asynx instance...")

	ax, err := build()
	if err != nil {
		return nil,
			nil,
			err
	}

	fmt.Println("Step 2: Registering event subscribers...")

	notifier := &projections.OrderNotifier{}

	err = subscribeAll(
		ax,
		&projections.OrderReporter{},
		notifier,
	)
	if err != nil {
		return nil,
			nil,
			err
	}

	return ax,
		notifier,
		nil
}

func run(
	ctx context.Context,
) error {
	fmt.Println("=== Asynx E-Commerce Order Example ===")

	ax, notifier, err := setup()
	if err != nil {
		return err
	}

	fmt.Println("Step 3: Executing commands...")

	orders := []string{"ORD-001", "ORD-002", "ORD-003"}

	sendPhases(
		ctx,
		ax,
		orders,
	)

	fmt.Println("Step 4: Verifying final state...")

	err = printOrders(
		ctx,
		ax,
		orders,
	)
	if err != nil {
		return err
	}

	printOutbox(notifier)

	return shutdown(
		ctx,
		ax,
	)
}

func main() {
	ctx := context.Background()

	err := run(ctx)
	if err != nil {
		panic(err)
	}
}

package main

import (
	"context"
	"encoding/json"
	"errors"
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
) error {
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
		return fmt.Errorf(
			"%s order %s: %w",
			step.verb,
			orderID,
			err,
		)
	}

	fmt.Printf(
		"✓ Order %s %s\n",
		orderID,
		step.past,
	)

	return nil
}

func sendAll(
	ctx context.Context,
	ax asynx.Asynx[domain.Order],
	orders []string,
	step phase,
) (err error) {
	for _, orderID := range orders {
		err = errors.Join(
			err,
			sendOne(
				ctx,
				ax,
				orderID,
				step,
			),
		)
	}

	ax.WaitPublish()
	fmt.Println()

	return err
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
		return fmt.Errorf(
			"load order %s: %w",
			orderID,
			err,
		)
	}

	return printJSON(
		orderID,
		order,
	)
}

func printJSON(
	orderID string,
	order domain.Order,
) error {
	orderJSON, err := json.MarshalIndent(
		order,
		"",
		"  ",
	)
	if err != nil {
		return fmt.Errorf(
			"encode order %s: %w",
			orderID,
			err,
		)
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
) (err error) {
	for _, orderID := range orders {
		err = errors.Join(
			err,
			printOrder(
				ctx,
				ax,
				orderID,
			),
		)
	}

	return err
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
) (err error) {
	steps := []phase{
		{verb: "create", past: "created", factory: createCmd},
		{verb: "confirm", past: "confirmed", factory: confirmCmd},
		{verb: "ship", past: "shipped", factory: shipCmd},
	}
	for _, step := range steps {
		err = errors.Join(
			err,
			sendAll(
				ctx,
				ax,
				orders,
				step,
			),
		)
	}

	return err
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

	sendErr := sendPhases(
		ctx,
		ax,
		orders,
	)

	fmt.Println("Step 4: Verifying final state...")

	printErr := printOrders(
		ctx,
		ax,
		orders,
	)

	printOutbox(notifier)

	return errors.Join(
		sendErr,
		printErr,
		shutdown(
			ctx,
			ax,
		),
	)
}

func main() {
	ctx := context.Background()

	err := run(ctx)
	if err != nil {
		panic(err)
	}
}

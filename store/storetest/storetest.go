package storetest

import (
	"context"
	"errors"
	"github.com/char2cs/asynx/models"
	"maps"
	"testing"
	"time"
)

// Factory returns a fresh, empty store for one subtest.
type Factory func(
	t *testing.T,
) models.Store

var errBoom = errors.New("storetest: boom")

// Run executes the full conformance suite against stores built by newStore.
func Run(
	t *testing.T,
	newStore Factory,
) {
	t.Helper()

	tests := storeScenarios()

	for name, fn := range tests {
		t.Run(
			name,
			scenarioCase(
				fn,
				newStore,
			),
		)
	}
}

func scenarioCase(
	fn scenario[models.Store],
	newStore Factory,
) func(*testing.T) {
	return func(
		t *testing.T,
	) {
		fn(
			t,
			newStore(t),
		)
	}
}

func storeScenarios() map[string]scenario[models.Store] {
	scenarios := map[string]scenario[models.Store]{}
	for _, group := range []map[string]scenario[models.Store]{
		appendScenarios(),
		concurrencyScenarios(),
		transactionScenarios(),
		deliveryScenarios(),
	} {
		maps.Copy(
			scenarios,
			group,
		)
	}

	return scenarios
}

const (
	listLimit       = 10
	wideLimit       = 100
	smallLimit      = 2
	versionTwo      = 2
	versionThree    = 3
	versionFour     = 4
	versionFive     = 5
	ghostVersion    = 7
	conflictVersion = 99
	readWait        = 100 * time.Millisecond
)

func opAppend(
	streamID string,
	version int64,
	data []byte,
) func(context.Context, models.Tx) error {
	return func(
		ctx context.Context,
		tx models.Tx,
	) error {
		return tx.Append(
			ctx,
			streamID,
			version,
			data,
		)
	}
}

func opEnqueue(
	d models.Delivery,
) func(context.Context, models.Tx) error {
	return func(
		ctx context.Context,
		tx models.Tx,
	) error {
		return tx.EnqueueDelivery(
			ctx,
			d.AggregateID,
			d.Version,
		)
	}
}

func opDrop(
	aggregateID string,
) func(context.Context, models.Tx) error {
	return func(
		ctx context.Context,
		tx models.Tx,
	) error {
		return tx.DropDeliveries(
			ctx,
			aggregateID,
		)
	}
}

func opReceipt(
	commandID string,
	r models.Receipt,
) func(context.Context, models.Tx) error {
	return func(
		ctx context.Context,
		tx models.Tx,
	) error {
		return tx.PutReceipt(
			ctx,
			commandID,
			r,
		)
	}
}

func opFail(
	err error,
) func(context.Context, models.Tx) error {
	return func(
		context.Context,
		models.Tx,
	) error {
		return err
	}
}

func runOps(
	ops ...func(context.Context, models.Tx) error,
) func(models.Tx) error {
	return func(
		tx models.Tx,
	) error {
		return applyOps(
			tx,
			ops,
		)
	}
}

func applyOps(
	tx models.Tx,
	ops []func(context.Context, models.Tx) error,
) error {
	if len(ops) == 0 {
		return nil
	}

	if err := ops[0](
		context.Background(),
		tx,
	); err != nil {
		return err
	}

	return applyOps(
		tx,
		ops[1:],
	)
}

func commit(
	t testing.TB,
	s models.Store,
	ops ...func(context.Context, models.Tx) error,
) {
	t.Helper()
	mustAtomically(
		t,
		s,
		runOps(ops...),
	)
}

func blobsOrFatal(
	t testing.TB,
	s models.Store,
	streamID string,
	from int64,
) [][]byte {
	t.Helper()
	blobs, err := s.ReadFrom(
		context.Background(),
		streamID,
		from,
	)
	if err != nil {
		t.Fatalf(
			"ReadFrom: %v",
			err,
		)
	}

	return blobs
}

func pendingOrFatal(
	t testing.TB,
	s models.Store,
	limit int,
) []models.Delivery {
	t.Helper()
	pending, err := s.PendingDeliveries(
		context.Background(),
		limit,
	)
	if err != nil {
		t.Fatalf(
			"PendingDeliveries: %v",
			err,
		)
	}

	return pending
}

func receiptOrFatal(
	t testing.TB,
	s models.Store,
	commandID string,
) (found bool) {
	t.Helper()
	_, found, err := s.GetReceipt(
		context.Background(),
		commandID,
	)
	if err != nil {
		t.Fatalf(
			"GetReceipt: %v",
			err,
		)
	}

	return found
}

func mustAtomically(
	t testing.TB,
	s models.Store,
	fn func(models.Tx) error,
) {
	t.Helper()
	if err := s.Atomically(
		context.Background(),
		fn,
	); err != nil {
		t.Fatalf(
			"Atomically: %v",
			err,
		)
	}
}

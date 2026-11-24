package storetest

import (
	"context"
	"errors"
	"github.com/char2cs/asynx/models"
	"slices"
	"testing"
)

func enqueueEach(
	ctx context.Context,
	tx models.Tx,
	deliveries ...models.Delivery,
) error {
	for _, d := range deliveries {
		if err := tx.EnqueueDelivery(
			ctx,
			d.AggregateID,
			d.Version,
		); err != nil {
			return err
		}
	}

	return nil
}

func receiptsDuplicates(
	t testing.TB,
	s models.Store,
) {
	ctx := context.Background()
	want := models.Receipt{Accepted: true, EventID: "e1", Version: 3}
	mustAtomically(
		t,
		s,
		func(
			tx models.Tx,
		) error {
			return tx.PutReceipt(
				ctx,
				"cmd-1",
				want,
			)
		},
	)

	got, found, err := s.GetReceipt(
		ctx,
		"cmd-1",
	)
	if err != nil ||
		!found ||
		got != want {
		t.Fatalf(
			"GetReceipt = %+v, %v, %v; want %+v",
			got,
			found,
			err,
			want,
		)
	}

	err = s.Atomically(
		ctx,
		func(
			tx models.Tx,
		) error {
			return tx.PutReceipt(
				ctx,
				"cmd-1",
				models.Receipt{Rejection: "nope"},
			)
		},
	)
	if !errors.Is(
		err,
		models.ErrDuplicateReceipt,
	) {
		t.Fatalf(
			"duplicate PutReceipt err = %v, want ErrDuplicateReceipt",
			err,
		)
	}

	if _, found, _ := s.GetReceipt(
		ctx,
		"missing",
	); found {
		t.Error("GetReceipt(missing) reported found")
	}
}

func deliveriesOrderAndAck(
	t testing.TB,
	s models.Store,
) {
	ctx := context.Background()
	want := []models.Delivery{{AggregateID: "a", Version: 1}, {AggregateID: "b", Version: 1}, {AggregateID: "a", Version: 2}}
	mustAtomically(
		t,
		s,
		func(
			tx models.Tx,
		) error {
			return enqueueEach(
				ctx,
				tx,
				want...,
			)
		},
	)

	pending, err := s.PendingDeliveries(
		ctx,
		10,
	)
	if err != nil {
		t.Fatalf(
			"PendingDeliveries: %v",
			err,
		)
	}

	if len(pending) != len(want) {
		t.Fatalf(
			"pending = %v, want %v",
			pending,
			want,
		)
	}

	for i := range want {
		if pending[i] != want[i] {
			t.Fatalf(
				"pending = %v, want %v",
				pending,
				want,
			)
		}
	}

	if err := s.AckDelivery(
		ctx,
		"b",
		1,
	); err != nil {
		t.Fatalf(
			"AckDelivery: %v",
			err,
		)
	}

	pending, _ = s.PendingDeliveries(
		ctx,
		10,
	)
	if len(pending) != 2 ||
		pending[0] != (models.Delivery{AggregateID: "a", Version: 1}) ||
		pending[1] != (models.Delivery{AggregateID: "a", Version: 2}) {
		t.Fatalf(
			"pending after ack = %v, want [{a 1} {a 2}]",
			pending,
		)
	}
}

func dropDeliveries(
	t testing.TB,
	s models.Store,
) {
	ctx := context.Background()
	mustAtomically(
		t,
		s,
		func(
			tx models.Tx,
		) error {
			_ = tx.EnqueueDelivery(
				ctx,
				"a",
				1,
			)
			_ = tx.EnqueueDelivery(
				ctx,
				"b",
				1,
			)
			_ = tx.EnqueueDelivery(
				ctx,
				"a",
				2,
			)
			return tx.DropDeliveries(
				ctx,
				"a",
			)
		},
	)

	pending, _ := s.PendingDeliveries(
		ctx,
		10,
	)
	if len(pending) != 1 ||
		pending[0] != (models.Delivery{AggregateID: "b", Version: 1}) {
		t.Fatalf(
			"pending = %v, want [{b 1}]",
			pending,
		)
	}
}

func ackUnknown(
	t testing.TB,
	s models.Store,
) {
	if err := s.AckDelivery(
		context.Background(),
		"ghost",
		7,
	); err != nil {
		t.Fatalf(
			"AckDelivery(unknown) = %v, want nil",
			err,
		)
	}
}

func wantPending(
	t testing.TB,
	s models.Store,
	want ...models.Delivery,
) {
	t.Helper()
	pending, err := s.PendingDeliveries(
		context.Background(),
		100,
	)
	if err != nil ||
		!slices.Equal(
			pending,
			want,
		) {
		t.Fatalf(
			"pending = %v (err %v), want %v",
			pending,
			err,
			want,
		)
	}
}

func enqueueIn(
	t testing.TB,
	s models.Store,
	fn func(ctx context.Context, tx models.Tx) error,
) {
	t.Helper()
	ctx := context.Background()
	mustAtomically(
		t,
		s,
		func(
			tx models.Tx,
		) error {
			return fn(
				ctx,
				tx,
			)
		},
	)
}

func dropThenEnqueue(
	t testing.TB,
	s models.Store,
) {
	enqueueIn(
		t,
		s,
		func(
			ctx context.Context,
			tx models.Tx,
		) error {
			return errors.Join(
				tx.EnqueueDelivery(
					ctx,
					"a",
					1,
				),
				tx.EnqueueDelivery(
					ctx,
					"b",
					1,
				),
			)
		},
	)
	enqueueIn(
		t,
		s,
		func(
			ctx context.Context,
			tx models.Tx,
		) error {
			return errors.Join(
				tx.DropDeliveries(
					ctx,
					"a",
				),
				tx.EnqueueDelivery(
					ctx,
					"a",
					2,
				),
			)
		},
	)
	wantPending(
		t,
		s,
		models.Delivery{AggregateID: "b", Version: 1},
		models.Delivery{AggregateID: "a", Version: 2},
	)
}

func reEnqueueAfterAckOrDrop(
	t testing.TB,
	s models.Store,
) {
	a1 := models.Delivery{AggregateID: "a", Version: 1}
	enqueue := func() {
		enqueueIn(
			t,
			s,
			func(
				ctx context.Context,
				tx models.Tx,
			) error {
				return tx.EnqueueDelivery(
					ctx,
					"a",
					1,
				)
			},
		)
	}
	enqueue()
	if err := s.AckDelivery(
		context.Background(),
		"a",
		1,
	); err != nil {
		t.Fatal(err)
	}

	enqueue()
	wantPending(
		t,
		s,
		a1,
	)
	enqueueIn(
		t,
		s,
		func(
			ctx context.Context,
			tx models.Tx,
		) error {
			return tx.DropDeliveries(
				ctx,
				"a",
			)
		},
	)
	enqueue()
	wantPending(
		t,
		s,
		a1,
	)
}

func ackAfterDrop(
	t testing.TB,
	s models.Store,
) {
	enqueueIn(
		t,
		s,
		func(
			ctx context.Context,
			tx models.Tx,
		) error {
			return errors.Join(
				tx.EnqueueDelivery(
					ctx,
					"a",
					1,
				),
				tx.DropDeliveries(
					ctx,
					"a",
				),
			)
		},
	)
	if err := s.AckDelivery(
		context.Background(),
		"a",
		1,
	); err != nil {
		t.Fatalf(
			"AckDelivery after drop = %v, want nil",
			err,
		)
	}
}

func pendingLimit(
	t testing.TB,
	s models.Store,
) {
	ctx := context.Background()
	mustAtomically(
		t,
		s,
		func(
			tx models.Tx,
		) error {
			return enqueueEach(
				ctx,
				tx,
				models.Delivery{AggregateID: "a", Version: 1},
				models.Delivery{AggregateID: "a", Version: 2},
				models.Delivery{AggregateID: "a", Version: 3},
				models.Delivery{AggregateID: "a", Version: 4},
				models.Delivery{AggregateID: "a", Version: 5},
			)
		},
	)
	pending, _ := s.PendingDeliveries(
		ctx,
		2,
	)
	if len(pending) != 2 ||
		pending[0].Version != 1 ||
		pending[1].Version != 2 {
		t.Fatalf(
			"PendingDeliveries(2) = %v, want versions 1,2",
			pending,
		)
	}
}

func deliveryScenarios() map[string]scenario[models.Store] {
	return map[string]scenario[models.Store]{
		"ReceiptsAreStoredAndDuplicatesRejected":  receiptsDuplicates,
		"DeliveriesKeepEnqueueOrderAndAckRemoves": deliveriesOrderAndAck,
		"DropDeliveriesOnlyAffectsThatAggregate":  dropDeliveries,
		"AckOfUnknownDeliveryIsNotAnError":        ackUnknown,
		"DropThenEnqueueInOneTxKeepsTheNewEntry":  dropThenEnqueue,
		"DeliveryKeyCanBeEnqueuedAgain":           reEnqueueAfterAckOrDrop,
		"AckAfterDropIsNotAnError":                ackAfterDrop,
		"PendingDeliveriesHonoursLimit":           pendingLimit,
	}
}

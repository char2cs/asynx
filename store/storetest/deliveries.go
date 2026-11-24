package storetest

import (
	"context"
	"errors"
	"github.com/char2cs/asynx/models"
	"slices"
	"testing"
)

func deliveryA1() models.Delivery {
	return models.Delivery{AggregateID: "a", Version: 1}
}

func deliveryA2() models.Delivery {
	return models.Delivery{AggregateID: "a", Version: versionTwo}
}

func deliveryB1() models.Delivery {
	return models.Delivery{AggregateID: "b", Version: 1}
}

func wantReceiptStored(
	t testing.TB,
	s models.Store,
	want models.Receipt,
) {
	got, found, err := s.GetReceipt(
		context.Background(),
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
}

func receiptsDuplicates(
	t testing.TB,
	s models.Store,
) {
	want := models.Receipt{Accepted: true, EventID: "e1", Version: versionThree}
	commit(
		t,
		s,
		opReceipt(
			"cmd-1",
			want,
		),
	)
	wantReceiptStored(
		t,
		s,
		want,
	)
	wantDuplicateRejected(
		t,
		s,
	)
	if receiptOrFatal(
		t,
		s,
		"missing",
	) {
		t.Error("GetReceipt(missing) reported found")
	}
}

func wantDuplicateRejected(
	t testing.TB,
	s models.Store,
) {
	err := s.Atomically(
		context.Background(),
		runOps(opReceipt(
			"cmd-1",
			models.Receipt{Rejection: "nope"},
		)),
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
}

func wantPendingPrefix(
	t testing.TB,
	pending []models.Delivery,
	want []models.Delivery,
) {
	if !slices.Equal(
		pending,
		want,
	) {
		t.Fatalf(
			"pending = %v, want %v",
			pending,
			want,
		)
	}
}

func deliveriesOrderAndAck(
	t testing.TB,
	s models.Store,
) {
	ctx := context.Background()
	want := []models.Delivery{deliveryA1(), deliveryB1(), deliveryA2()}
	commit(
		t,
		s,
		opEnqueue(want[0]),
		opEnqueue(want[1]),
		opEnqueue(want[2]),
	)
	wantPendingPrefix(
		t,
		pendingOrFatal(
			t,
			s,
			listLimit,
		),
		want,
	)
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

	wantAfterAck(
		t,
		s,
	)
}

func wantAfterAck(
	t testing.TB,
	s models.Store,
) {
	pending := pendingOrFatal(
		t,
		s,
		listLimit,
	)
	if !slices.Equal(
		pending,
		[]models.Delivery{deliveryA1(), deliveryA2()},
	) {
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
	commit(
		t,
		s,
		opEnqueue(deliveryA1()),
		opEnqueue(deliveryB1()),
		opEnqueue(deliveryA2()),
		opDrop("a"),
	)
	pending := pendingOrFatal(
		t,
		s,
		listLimit,
	)
	if !slices.Equal(
		pending,
		[]models.Delivery{deliveryB1()},
	) {
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
		ghostVersion,
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
		wideLimit,
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

func dropThenEnqueue(
	t testing.TB,
	s models.Store,
) {
	commit(
		t,
		s,
		opEnqueue(deliveryA1()),
		opEnqueue(deliveryB1()),
	)
	commit(
		t,
		s,
		opDrop("a"),
		opEnqueue(deliveryA2()),
	)
	wantPending(
		t,
		s,
		deliveryB1(),
		deliveryA2(),
	)
}

func ackA1(
	t testing.TB,
	s models.Store,
) {
	if err := s.AckDelivery(
		context.Background(),
		"a",
		1,
	); err != nil {
		t.Fatal(err)
	}
}

func reEnqueueAfterAckOrDrop(
	t testing.TB,
	s models.Store,
) {
	commit(
		t,
		s,
		opEnqueue(deliveryA1()),
	)
	ackA1(
		t,
		s,
	)
	commit(
		t,
		s,
		opEnqueue(deliveryA1()),
	)
	wantPending(
		t,
		s,
		deliveryA1(),
	)
	commit(
		t,
		s,
		opDrop("a"),
	)
	commit(
		t,
		s,
		opEnqueue(deliveryA1()),
	)
	wantPending(
		t,
		s,
		deliveryA1(),
	)
}

func ackAfterDrop(
	t testing.TB,
	s models.Store,
) {
	commit(
		t,
		s,
		opEnqueue(deliveryA1()),
		opDrop("a"),
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
	commit(
		t,
		s,
		opEnqueue(deliveryA1()),
		opEnqueue(deliveryA2()),
		opEnqueue(models.Delivery{AggregateID: "a", Version: versionThree}),
		opEnqueue(models.Delivery{AggregateID: "a", Version: versionFour}),
		opEnqueue(models.Delivery{AggregateID: "a", Version: versionFive}),
	)
	pending := pendingOrFatal(
		t,
		s,
		smallLimit,
	)
	if len(pending) != smallLimit ||
		pending[0].Version != 1 ||
		pending[1].Version != versionTwo {
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

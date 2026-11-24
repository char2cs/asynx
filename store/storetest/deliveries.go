package storetest

import (
	"errors"
	"github.com/char2cs/asynx/v2/models/event"
	"github.com/char2cs/asynx/v2/models/failure"
	"github.com/char2cs/asynx/v2/models/storage"
	"github.com/char2cs/asynx/v2/store/storetest/internal/write"
	"testing"
)

func receiptsDuplicates(
	t testing.TB,
	s storage.Store,
) {
	f := newFixture(
		t,
		s,
	)
	want := event.Receipt{Accepted: true, EventID: "e1", Version: 1}
	f.commit(write.Receipt(
		"cmd-1",
		want,
	))
	f.wantReceipt(
		"cmd-1",
		want,
	)

	err := f.try(write.Receipt(
		"cmd-1",
		event.Receipt{Rejection: "nope"},
	))
	if !errors.Is(
		err,
		failure.ErrDuplicateReceipt,
	) {
		t.Fatalf(
			"duplicate PutReceipt err = %v, want ErrDuplicateReceipt",
			err,
		)
	}

	f.wantNoReceipt("missing")
}

func deliveriesOrderAndAck(
	t testing.TB,
	s storage.Store,
) {
	a1 := deliveryOf(
		"a",
		1,
	)
	b1 := deliveryOf(
		"b",
		1,
	)
	a2 := successor(a1)
	f := newFixture(
		t,
		s,
	)
	f.commit(
		write.Delivery(a1),
		write.Delivery(b1),
		write.Delivery(a2),
	)
	f.wantPending(
		a1,
		b1,
		a2,
	)

	f.ack(
		b1.AggregateID,
		b1.Version,
	)
	f.wantPending(
		a1,
		a2,
	)
}

func dropDeliveries(
	t testing.TB,
	s storage.Store,
) {
	a1 := deliveryOf(
		"a",
		1,
	)
	b1 := deliveryOf(
		"b",
		1,
	)
	f := newFixture(
		t,
		s,
	)
	f.commit(
		write.Delivery(a1),
		write.Delivery(b1),
		write.Delivery(successor(a1)),
		write.EraseDeliveries("a"),
	)

	f.wantPending(b1)
}

func ackUnknown(
	t testing.TB,
	s storage.Store,
) {
	const unknownVersion = 7
	newFixture(
		t,
		s,
	).ack(
		"ghost",
		unknownVersion,
	)
}

func dropThenEnqueue(
	t testing.TB,
	s storage.Store,
) {
	a1 := deliveryOf(
		"a",
		1,
	)
	b1 := deliveryOf(
		"b",
		1,
	)
	f := newFixture(
		t,
		s,
	)
	f.commit(
		write.Delivery(a1),
		write.Delivery(b1),
	)
	f.commit(
		write.EraseDeliveries("a"),
		write.Delivery(successor(a1)),
	)

	f.wantPending(
		b1,
		successor(a1),
	)
}

func reEnqueueAfterAckOrDrop(
	t testing.TB,
	s storage.Store,
) {
	a1 := deliveryOf(
		"a",
		1,
	)
	f := newFixture(
		t,
		s,
	)
	f.commit(write.Delivery(a1))
	f.ack(
		a1.AggregateID,
		a1.Version,
	)
	f.commit(write.Delivery(a1))
	f.wantPending(a1)

	f.commit(write.EraseDeliveries("a"))
	f.commit(write.Delivery(a1))
	f.wantPending(a1)
}

func ackAfterDrop(
	t testing.TB,
	s storage.Store,
) {
	a1 := deliveryOf(
		"a",
		1,
	)
	f := newFixture(
		t,
		s,
	)
	f.commit(
		write.Delivery(a1),
		write.EraseDeliveries("a"),
	)

	f.ack(
		a1.AggregateID,
		a1.Version,
	)
}

func pendingLimit(
	t testing.TB,
	s storage.Store,
) {
	const (
		queued = 5
		limit  = 2
	)
	f := newFixture(
		t,
		s,
	)
	f.commit(write.Deliveries(
		"a",
		queued,
	)...)

	got := f.pending(limit)

	a1 := deliveryOf(
		"a",
		1,
	)
	if len(got) != limit ||
		got[0] != a1 ||
		got[1] != successor(a1) {
		t.Fatalf(
			"PendingDeliveries(%d) = %v, want the first two enqueued",
			limit,
			got,
		)
	}
}

func deliveryScenarios() map[string]scenario[storage.Store] {
	return map[string]scenario[storage.Store]{
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

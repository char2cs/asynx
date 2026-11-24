package write_test

import (
	"context"
	"github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
	"github.com/char2cs/asynx/store/storetest/internal/write"
	"slices"
	"testing"
)

func TestDeliveries_EnqueueVersionsOneUpwardInOrder(
	t *testing.T,
) {
	s := store.New()
	const count = 3

	err := s.Atomically(
		context.Background(),
		write.Transaction(write.Deliveries(
			"a",
			count,
		)...),
	)
	if err != nil {
		t.Fatalf(
			"Atomically = %v, want nil",
			err,
		)
	}

	got := pendingOf(
		t,
		s,
		count+1,
	)
	want := []models.Delivery{
		{AggregateID: "a", Version: 1},
		{AggregateID: "a", Version: 2},
		{AggregateID: "a", Version: 3},
	}
	if !slices.Equal(
		got,
		want,
	) {
		t.Fatalf(
			"pending = %v, want %v",
			got,
			want,
		)
	}
}

func TestEraseDeliveries_DropsOnlyThatAggregatesEntries(
	t *testing.T,
) {
	s := store.New()
	b1 := models.Delivery{AggregateID: "b", Version: 1}
	seed := write.Transaction(
		write.Delivery(models.Delivery{AggregateID: "a", Version: 1}),
		write.Delivery(b1),
		write.EraseDeliveries("a"),
	)

	err := s.Atomically(
		context.Background(),
		seed,
	)
	if err != nil {
		t.Fatalf(
			"Atomically = %v, want nil",
			err,
		)
	}

	got := pendingOf(
		t,
		s,
		2,
	)
	if !slices.Equal(
		got,
		[]models.Delivery{b1},
	) {
		t.Fatalf(
			"pending = %v, want only b's entry",
			got,
		)
	}
}

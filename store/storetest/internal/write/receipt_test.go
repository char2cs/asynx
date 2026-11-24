package write_test

import (
	"context"
	"github.com/char2cs/asynx/v2/models/event"
	"github.com/char2cs/asynx/v2/store"
	"github.com/char2cs/asynx/v2/store/storetest/internal/write"
	"testing"
)

func TestReceipt_StoresTheReceiptUnderTheCommandID(
	t *testing.T,
) {
	s := store.New()
	want := event.Receipt{Accepted: true, EventID: "e1", Version: 1}

	err := s.Atomically(
		context.Background(),
		write.Transaction(write.Receipt(
			"cmd-1",
			want,
		)),
	)
	if err != nil {
		t.Fatalf(
			"Atomically = %v, want nil",
			err,
		)
	}

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

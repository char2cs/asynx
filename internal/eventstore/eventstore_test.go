package eventstore_test

import (
	"context"
	"testing"

	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/store"
)

func TestNew_WritesAndReadsBack(
	t *testing.T,
) {
	es := eventstore.New[mocks.Order](
		store.New(),
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	ctx := context.Background()

	_, err := es.Write(
		ctx,
		mocks.UpdateOrderCmd{
			ID:       "agg1",
			NewState: mocks.Order{ID: "1", Status: "Pending"},
		},
	)
	if err != nil {
		t.Fatalf(
			"Write: %v",
			err,
		)
	}

	got, err := es.Get(
		ctx,
		"agg1",
	)
	if err != nil {
		t.Fatalf(
			"Get: %v",
			err,
		)
	}

	if got.Status != "Pending" {
		t.Fatalf(
			"Status = %q, want Pending",
			got.Status,
		)
	}
}

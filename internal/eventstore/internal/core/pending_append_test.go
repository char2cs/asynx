package core

import (
	"context"
	"testing"

	"github.com/char2cs/asynx/store"
)

func TestPendingAppend_InTransactionKeepsTheAppendedEvent(
	t *testing.T,
) {
	es := New[order](
		store.New(),
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	ctx := context.Background()
	pending := &pendingAppend[order]{
		store:         es,
		cmd:           cmd(order{ID: "1", Status: "Pending"}),
		newState:      order{ID: "1", Status: "Pending"},
		loadedVersion: 0,
	}

	err := es.Store().Atomically(
		ctx,
		pending.inTransaction(ctx),
	)
	if err != nil {
		t.Fatalf(
			"Atomically: %v",
			err,
		)
	}

	if pending.evt.Version != 1 ||
		pending.evt.Aggregate.Status != "Pending" {
		t.Fatalf(
			"kept event = %+v, want version 1 carrying the new state",
			pending.evt,
		)
	}
}

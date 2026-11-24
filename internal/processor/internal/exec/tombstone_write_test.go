package exec

import (
	"context"
	"errors"
	"testing"

	"github.com/char2cs/asynx/models/failure"
)

func TestTombstoneWrite_CommitRaisesTheFenceAndKeepsTheTicket(
	t *testing.T,
) {
	ex, _, _, _, _ := newEnv(t)
	ctx := context.Background()
	mustExecute(
		t,
		ex,
		update(
			"agg",
			1,
		),
	)

	written := &tombstoneWrite[order]{executor: ex}
	err := written.commit(
		ctx,
		"agg",
	)
	if err != nil {
		t.Fatalf(
			"commit: %v",
			err,
		)
	}

	if !ex.isForgetting("agg") {
		t.Fatal("the fence was not raised after the tombstone was written")
	}

	if written.ticket.Final == nil {
		t.Fatal("the ticket of the enqueued tombstone was not kept")
	}
}

func TestTombstoneWrite_CommitRefusesAnAggregateAlreadyBeingForgotten(
	t *testing.T,
) {
	ex, _, _, _, _ := newEnv(t)
	mustExecute(
		t,
		ex,
		update(
			"agg",
			1,
		),
	)
	ex.markForgetting("agg")

	written := &tombstoneWrite[order]{executor: ex}
	err := written.commit(
		context.Background(),
		"agg",
	)
	if !errors.Is(
		err,
		failure.ErrForgetInProgress,
	) {
		t.Fatalf(
			"commit = %v, want ErrForgetInProgress",
			err,
		)
	}
}

func TestTombstoneWrite_CommitFailureLowersTheFenceAndKeepsNoTicket(
	t *testing.T,
) {
	ex, _, _, _, _ := newEnv(t)

	written := &tombstoneWrite[order]{executor: ex}
	err := written.commit(
		context.Background(),
		"ghost",
	)
	if !errors.Is(
		err,
		failure.ErrValidation,
	) {
		t.Fatalf(
			"commit = %v, want ErrValidation for an aggregate that does not exist",
			err,
		)
	}

	if ex.isForgetting("ghost") {
		t.Fatal("the fence stayed up after the failed tombstone")
	}

	if written.ticket.Final != nil {
		t.Fatal("a failed commit kept a ticket")
	}
}

func TestTombstoneWrite_CommitHonoursTheContext(
	t *testing.T,
) {
	ex, _, _, _, _ := newEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	written := &tombstoneWrite[order]{executor: ex}
	err := written.commit(
		ctx,
		"agg",
	)
	if !errors.Is(
		err,
		failure.ErrContextCancelled,
	) {
		t.Fatalf(
			"commit = %v, want ErrContextCancelled",
			err,
		)
	}

	if ex.isForgetting("agg") {
		t.Fatal("the fence was raised although the lock was never taken")
	}
}

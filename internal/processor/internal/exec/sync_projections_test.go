package exec

import (
	"context"
	"errors"
	"fmt"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/processor/models"
	"github.com/char2cs/asynx/models/event"
	"github.com/char2cs/asynx/models/failure"
	"github.com/char2cs/asynx/models/storage"
	"testing"
)

func TestExecute_SyncProjectionErrorRollsBackAndDispatchesNothing(
	t *testing.T,
) {
	boom := errors.New("read model write failed")
	failing := &failingProjection{err: boom}
	ex, s, _, d, b := newEnv(
		t,
		syncAll(failing.project),
	)
	delivered := &eventCounter{}
	b.Subscribe(
		"OrderUpdated",
		delivered.onEvent,
	)

	_, err := ex.Execute(
		context.Background(),
		update(
			"agg",
			1,
		),
	)
	d.WaitIdle()

	if !errors.Is(
		err,
		failure.ErrPipelineFailed,
	) ||
		!errors.Is(
			err,
			failure.ErrSyncProjection,
		) ||
		!errors.Is(
			err,
			boom,
		) {
		t.Fatalf(
			"err = %v, want ErrPipelineFailed + ErrSyncProjection + cause",
			err,
		)
	}

	if blobs, _ := s.ReadFrom(
		context.Background(),
		"events:agg",
		1,
	); len(blobs) != 0 {
		t.Errorf(
			"event survived a failed sync projection: %d blobs",
			len(blobs),
		)
	}

	if delivered.calls.Load() != 0 {
		t.Errorf(
			"deliveries = %d, want 0",
			delivered.calls.Load(),
		)
	}
}

type failingProjection struct {
	err error
}

func (p *failingProjection) project(
	context.Context,
	storage.Tx,
	event.Event[order],
) error {
	return p.err
}

type seenProjection struct {
	got []string
}

func (p *seenProjection) record(
	_ context.Context,
	tx storage.Tx,
	e event.Event[order],
) error {
	if tx == nil {
		return errors.New("nil tx")
	}

	p.got = append(
		p.got,
		fmt.Sprintf(
			"%s@%d",
			e.AggregateID,
			e.Version,
		),
	)
	return nil
}

func TestExecute_SyncProjectionSeesTxAndEvent_OnlyForMatchingEvents(
	t *testing.T,
) {
	seen := &seenProjection{}
	ex, _, _, _, _ := newEnv(
		t,
		WithSyncProjections[order]([]models.SyncProjection[order]{{
			Match: matchUpdated,
			Fn:    seen.record,
		}}),
	)

	if _, err := ex.Execute(
		context.Background(),
		update(
			"agg",
			1,
		),
	); err != nil {
		t.Fatal(err)
	}

	if _, err := ex.Execute(
		context.Background(),
		mocks.CreateOrderCmd{ID: "other", Total: 1},
	); err != nil {
		t.Fatal(err)
	}

	if len(seen.got) != 1 ||
		seen.got[0] != "agg@1" {
		t.Fatalf(
			"projection calls = %v, want [agg@1]",
			seen.got,
		)
	}
}

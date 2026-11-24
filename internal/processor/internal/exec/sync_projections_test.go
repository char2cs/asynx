package exec

import (
	"context"
	"errors"
	"fmt"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/processor/models"
	asynxmd "github.com/char2cs/asynx/models"
	"sync/atomic"
	"testing"
)

func TestExecute_SyncProjectionErrorRollsBackAndDispatchesNothing(
	t *testing.T,
) {
	boom := errors.New("read model write failed")
	failing := func(
		context.Context,
		asynxmd.Tx,
		asynxmd.Event[order],
	) error {
		return boom
	}
	ex, s, _, d, b := newEnv(
		t,
		syncAll(failing),
	)
	var delivered atomic.Int32
	count := func(
		context.Context,
		asynxmd.Event[order],
	) {
		delivered.Add(1)
	}
	b.Subscribe(
		"OrderUpdated",
		count,
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
		asynxmd.ErrPipelineFailed,
	) ||
		!errors.Is(
			err,
			asynxmd.ErrSyncProjection,
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

	if delivered.Load() != 0 {
		t.Errorf(
			"deliveries = %d, want 0",
			delivered.Load(),
		)
	}
}

func appendSeen(
	got []string,
	tx asynxmd.Tx,
	e asynxmd.Event[order],
) ([]string, error) {
	if tx == nil {
		return got,
			errors.New("nil tx")
	}

	return append(
			got,
			fmt.Sprintf(
				"%s@%d",
				e.AggregateID,
				e.Version,
			),
		),
		nil
}

func TestExecute_SyncProjectionSeesTxAndEvent_OnlyForMatchingEvents(
	t *testing.T,
) {
	var got []string
	match := func(
		name string,
	) bool {
		return name == "OrderUpdated"
	}
	record := func(
		_ context.Context,
		tx asynxmd.Tx,
		e asynxmd.Event[order],
	) (err error) {
		got, err = appendSeen(
			got,
			tx,
			e,
		)
		return err
	}
	ex, _, _, _, _ := newEnv(
		t,
		WithSyncProjections[order]([]models.SyncProjection[order]{{
			Match: match,
			Fn:    record,
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

	if len(got) != 1 ||
		got[0] != "agg@1" {
		t.Fatalf(
			"projection calls = %v, want [agg@1]",
			got,
		)
	}
}

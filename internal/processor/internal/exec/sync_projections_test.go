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
	ex, s, _, d, b := newEnv(
		t,
		syncAll(func(
			context.Context,
			asynxmd.Tx,
			asynxmd.Event[order],
		) error {
			return boom
		}),
	)
	var delivered atomic.Int32
	b.Subscribe(
		"OrderUpdated",
		func(
			context.Context,
			asynxmd.Event[order],
		) {
			delivered.Add(1)
		},
	)

	_, err := ex.Execute(
		context.Background(),
		update(
			"agg",
			1,
		),
		false,
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

func TestExecute_SyncProjectionSeesTxAndEvent_OnlyForMatchingEvents(
	t *testing.T,
) {
	var got []string
	ex, _, _, _, _ := newEnv(
		t,
		WithSyncProjections[order]([]models.SyncProjection[order]{{
			Match: func(
				name string,
			) bool {
				return name == "OrderUpdated"
			},
			Fn: func(
				_ context.Context,
				tx asynxmd.Tx,
				e asynxmd.Event[order],
			) error {
				if tx == nil {
					return errors.New("nil tx")
				}

				got = append(
					got,
					fmt.Sprintf(
						"%s@%d",
						e.AggregateID,
						e.Version,
					),
				)
				return nil
			},
		}}),
	)

	if _, err := ex.Execute(
		context.Background(),
		update(
			"agg",
			1,
		),
		false,
	); err != nil {
		t.Fatal(err)
	}

	if _, err := ex.Execute(
		context.Background(),
		mocks.CreateOrderCmd{ID: "other", Total: 1},
		false,
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

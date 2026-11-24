package asynx_test

import (
	"context"
	"errors"
	"github.com/char2cs/asynx"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
	"sync"
	"testing"
)

func TestGuarantee_SyncProjectionPatternsRegexAndOrder(
	t *testing.T,
) {
	var mu sync.Mutex
	var order []string
	rec := func(
		name string,
	) func(context.Context, models.Tx, models.Event[mocks.Order]) error {
		return func(
			_ context.Context,
			_ models.Tx,
			e models.Event[mocks.Order],
		) error {
			mu.Lock()
			order = append(
				order,
				name+":"+e.EventName,
			)
			mu.Unlock()
			return nil
		}
	}
	inst := build(
		t,
		eight(store.New()).
			WithSyncProjection(
				"^Order(Created|Updated)$",
				rec("regex"),
			).
			WithSyncProjection(
				"OrderUpdated",
				rec("exact"),
			),
	)

	ctx := context.Background()
	inst.Send(
		ctx,
		mocks.CreateOrderCmd{ID: "a", Total: 1},
	)
	inst.Send(
		ctx,
		upd(
			"a",
			2,
		),
	)

	mu.Lock()
	defer mu.Unlock()
	want := []string{"regex:OrderCreated", "regex:OrderUpdated", "exact:OrderUpdated"}
	if len(order) != len(want) {
		t.Fatalf(
			"calls = %v, want %v",
			order,
			want,
		)
	}

	for i := range want {
		if order[i] != want[i] {
			t.Fatalf(
				"calls = %v, want %v",
				order,
				want,
			)
		}
	}
}

func TestGuarantee_BuildRejectsBadProjectionsAndSubscribers(
	t *testing.T,
) {
	noop := func(
		context.Context,
		models.Tx,
		models.Event[mocks.Order],
	) error {
		return nil
	}
	cases := map[string]struct {
		b    *asynx.Builder[mocks.Order]
		want error
	}{
		"empty projection pattern": {eight(store.New()).WithSyncProjection(
			"",
			noop,
		), models.ErrEmptyPattern},
		"nil projection fn": {eight(store.New()).WithSyncProjection(
			"X",
			nil,
		), models.ErrNilHandler},
		"empty subscriber pattern": {eight(store.New()).WithSubscriber(
			"",
			func(
				context.Context,
				models.Event[mocks.Order],
			) error {
				return nil
			},
		), models.ErrEmptyPattern},
		"nil subscriber handler": {eight(store.New()).WithSubscriber(
			"X",
			nil,
		), models.ErrNilHandler},
	}
	for name, tc := range cases {
		t.Run(
			name,
			func(
				t *testing.T,
			) {
				if _, err := tc.b.Build(); !errors.Is(
					err,
					tc.want,
				) {
					t.Fatalf(
						"Build err = %v, want %v",
						err,
						tc.want,
					)
				}
			},
		)
	}

	if _, err := eight(store.New()).WithSyncProjection(
		"(unclosed",
		noop,
	).Build(); err == nil {
		t.Fatal("invalid regexp pattern was accepted")
	}

	if _, err := eight(store.New()).WithSubscriber(
		"(unclosed",
		func(
			context.Context,
			models.Event[mocks.Order],
		) error {
			return nil
		},
	).Build(); err == nil {
		t.Fatal("invalid subscriber pattern was accepted")
	}
}

func TestGuarantee_DurableFallbackIsRejectedAtBuild(
	t *testing.T,
) {
	_, err := eight(store.New()).
		WithSubscriber(
			"OrderUpdated",
			func(
				context.Context,
				models.Event[mocks.Order],
			) error {
				return nil
			},
			models.WithFallback(func(
				context.Context,
				models.Event[mocks.Order],
			) {
			}),
		).
		Build()
	if !errors.Is(
		err,
		models.ErrFallbackUnsupported,
	) {
		t.Fatalf(
			"Build err = %v, want ErrFallbackUnsupported",
			err,
		)
	}
}

func TestBuild_AnInvalidSubscriberStopsTheValidationOfTheNextOnes(
	t *testing.T,
) {
	handler := func(
		context.Context,
		models.Event[mocks.Order],
	) error {
		return nil
	}
	optionRan := false
	option := func(
		*models.SubscriptionConfig[mocks.Order],
	) {
		optionRan = true
	}

	inst, err := eight(store.New()).
		WithSubscriber(
			"",
			handler,
		).
		WithSubscriber(
			"OrderCreated",
			handler,
			option,
		).
		Build()
	if inst != nil ||
		!errors.Is(
			err,
			models.ErrEmptyPattern,
		) {
		t.Fatalf(
			"Build = (%v, %v), want ErrEmptyPattern and no instance",
			inst,
			err,
		)
	}

	if optionRan {
		t.Error("the options of a later subscriber ran after an earlier one was rejected")
	}
}

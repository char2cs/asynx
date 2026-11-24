package asynx_test

import (
	"context"
	"errors"
	"github.com/char2cs/asynx"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
	"slices"
	"sync"
	"testing"
)

type orderRecorder struct {
	mu    sync.Mutex
	order []string
}

func (r *orderRecorder) rec(
	name string,
) func(context.Context, models.Tx, models.Event[mocks.Order]) error {
	return func(
		_ context.Context,
		_ models.Tx,
		e models.Event[mocks.Order],
	) error {
		r.add(name + ":" + e.EventName)
		return nil
	}
}

func (r *orderRecorder) add(
	entry string,
) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.order = append(
		r.order,
		entry,
	)
}

func (r *orderRecorder) calls() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append(
		[]string(nil),
		r.order...,
	)
}

func requireCalls(
	t *testing.T,
	got []string,
	want []string,
) {
	t.Helper()

	if !slices.Equal(
		got,
		want,
	) {
		t.Fatalf(
			"calls = %v, want %v",
			got,
			want,
		)
	}
}

func TestGuarantee_SyncProjectionPatternsRegexAndOrder(
	t *testing.T,
) {
	recorder := &orderRecorder{}
	inst := build(
		t,
		eight(store.New()).
			WithSyncProjection(
				"^Order(Created|Updated)$",
				recorder.rec("regex"),
			).
			WithSyncProjection(
				"OrderUpdated",
				recorder.rec("exact"),
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

	requireCalls(
		t,
		recorder.calls(),
		[]string{"regex:OrderCreated", "regex:OrderUpdated", "exact:OrderUpdated"},
	)
}

func noopSyncProjection(
	context.Context,
	models.Tx,
	models.Event[mocks.Order],
) error {
	return nil
}

func noopSubscriber(
	context.Context,
	models.Event[mocks.Order],
) error {
	return nil
}

func requireBuildErr(
	t *testing.T,
	b *asynx.Builder[mocks.Order],
	want error,
) {
	t.Helper()

	_, err := b.Build()
	if !errors.Is(
		err,
		want,
	) {
		t.Fatalf(
			"Build err = %v, want %v",
			err,
			want,
		)
	}
}

func requireBuildFails(
	t *testing.T,
	b *asynx.Builder[mocks.Order],
	msg string,
) {
	t.Helper()

	_, err := b.Build()
	if err == nil {
		t.Fatal(msg)
	}
}

type buildCase struct {
	b    *asynx.Builder[mocks.Order]
	want error
}

func (c buildCase) run(
	t *testing.T,
) {
	requireBuildErr(
		t,
		c.b,
		c.want,
	)
}

func TestGuarantee_BuildRejectsBadProjectionsAndSubscribers(
	t *testing.T,
) {
	cases := map[string]buildCase{
		"empty projection pattern": {eight(store.New()).WithSyncProjection(
			"",
			noopSyncProjection,
		), models.ErrEmptyPattern},
		"nil projection fn": {eight(store.New()).WithSyncProjection(
			"X",
			nil,
		), models.ErrNilHandler},
		"empty subscriber pattern": {eight(store.New()).WithSubscriber(
			"",
			noopSubscriber,
		), models.ErrEmptyPattern},
		"nil subscriber handler": {eight(store.New()).WithSubscriber(
			"X",
			nil,
		), models.ErrNilHandler},
	}
	for name, tc := range cases {
		t.Run(
			name,
			tc.run,
		)
	}

	requireBuildFails(
		t,
		eight(store.New()).WithSyncProjection(
			"(unclosed",
			noopSyncProjection,
		),
		"invalid regexp pattern was accepted",
	)
	requireBuildFails(
		t,
		eight(store.New()).WithSubscriber(
			"(unclosed",
			noopSubscriber,
		),
		"invalid subscriber pattern was accepted",
	)
}

func noopFallback(
	context.Context,
	models.Event[mocks.Order],
) {
}

func TestGuarantee_DurableFallbackIsRejectedAtBuild(
	t *testing.T,
) {
	_, err := eight(store.New()).
		WithSubscriber(
			"OrderUpdated",
			noopSubscriber,
			models.WithFallback(noopFallback),
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

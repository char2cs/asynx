package processor_test

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/char2cs/asynx/internal/bus"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/processor"
	asynxmd "github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
)

func TestProcessor_Inspect_FailedResumeKeepsTheCauseAndUndelivered(t *testing.T) {
	var healthy atomic.Bool
	parked := make(chan struct{}, 1)
	s := &failingReadsStore{Memory: store.New()}
	f := newFixture(t, s, parkingOpts(&healthy, parkedSignal(parked))...)
	parkWithTwoCommands(t, f, parked)
	before := f.p.Inspect("o1")
	if !before.Parked || !errors.Is(before.Cause, asynxmd.ErrDeliveryParked) {
		t.Fatalf("Inspect before Resume = %+v, want parked by the give-up", before)
	}

	s.fail.Store(true)
	if err := f.p.Resume(context.Background(), 10); err == nil {
		t.Fatal("Resume hid the unreadable stream")
	}
	s.fail.Store(false)

	got := f.p.Inspect("o1")
	if !got.Parked || got.Generation == before.Generation {
		t.Fatalf("Inspect = %+v, want parked again with a new generation (was %d)", got, before.Generation)
	}
	if got.Cause == nil || !strings.Contains(got.Cause.Error(), "stream unreadable") {
		t.Fatalf("Cause = %v, want the redelivery error", got.Cause)
	}
	if errors.Is(got.Cause, asynxmd.ErrDeliveryParked) {
		t.Fatalf("Cause = %v, still the lifted park's give-up error", got.Cause)
	}
	if !slices.Contains(got.Undelivered, 2) || slices.Contains(got.Undelivered, 1) {
		t.Fatalf("Undelivered = %v, want v2 (no handler took it) and not v1 (its handler ran)", got.Undelivered)
	}
}

func TestProcessor_Inspect_FailedRecoverParksWithTheRedeliveryError(t *testing.T) {
	ctx := context.Background()
	s := store.New()
	es := eventstore.New[order](s, store.NewSnapshots(), nil, 1, nil)
	seedUnrebuildable(t, s, "bad")
	p := mustNew(t, es, bus.NewChannelBus[order](),
		processor.WithPublishErrorHandler(func(context.Context, asynxmd.Event[order], error) {}),
		processor.WithDurableSubscribers([]asynxmd.DurableSub[order]{{
			Match:   always,
			Handler: func(context.Context, asynxmd.Event[order]) error { return nil },
		}}))
	t.Cleanup(func() { p.Shutdown(ctx) })

	if err := p.Recover(ctx, 10); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	got := p.Inspect("bad")
	if !got.Parked || got.Cause == nil || errors.Is(got.Cause, asynxmd.ErrDeliveryParked) {
		t.Fatalf("Inspect = %+v, want parked with the redelivery error", got)
	}
}

func TestProcessor_Inspect_WithoutDispatcherIsTheZeroStatus(t *testing.T) {
	p := mustNew[order](t, newES(), nil)
	t.Cleanup(func() { p.Shutdown(context.Background()) })

	if got := p.Inspect("a"); !reflect.DeepEqual(got, asynxmd.DeliveryStatus{}) {
		t.Fatalf("Inspect = %+v, want the zero status", got)
	}
}

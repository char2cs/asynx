package core

import (
	"context"
	"errors"
	"github.com/char2cs/asynx/internal/bus"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/mockstores"
	"github.com/char2cs/asynx/internal/processor/models"
	asynxmd "github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestProcessor_Recover_RedeliversEveryPendingEventAndAcksIt(
	t *testing.T,
) {
	s := store.New()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	seedPending(
		t,
		s,
		es,
		"a",
		"b",
		"c",
	)

	b := bus.New[order]()
	var live atomic.Int32
	b.Subscribe(
		"OrderCreated",
		func(
			context.Context,
			asynxmd.Event[order],
		) {
			live.Add(1)
		},
	)
	var got atomic.Int32
	p := mustNew(
		t,
		es,
		b,
		models.WithDurableSubscribers([]asynxmd.DurableSub[order]{{
			Match: always,
			Handler: func(
				context.Context,
				asynxmd.Event[order],
			) error {
				got.Add(1)
				return nil
			},
		}}),
	)
	t.Cleanup(func() { p.Shutdown(context.Background()) })

	if err := p.Recover(
		context.Background(),
		2,
	); err != nil {
		t.Fatalf(
			"Recover: %v",
			err,
		)
	}

	p.WaitPublish()
	if got.Load() != 3 {
		t.Errorf(
			"redelivered %d events, want 3",
			got.Load(),
		)
	}

	if live.Load() != 0 {
		t.Errorf(
			"live subscribers saw %d recovered events, want none",
			live.Load(),
		)
	}

	if pending, _ := s.PendingDeliveries(
		context.Background(),
		10,
	); len(pending) != 0 {
		t.Errorf(
			"pending = %v, want none",
			pending,
		)
	}

	if err := p.Recover(
		context.Background(),
		0,
	); err != nil {
		t.Errorf(
			"Recover on an empty log: %v",
			err,
		)
	}
}

func TestProcessor_Recover_WithoutBusIsNoOp(
	t *testing.T,
) {
	s := store.New()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	p := mustNew[order](
		t,
		es,
		nil,
	)
	t.Cleanup(func() { p.Shutdown(context.Background()) })
	if err := p.Recover(
		context.Background(),
		10,
	); err != nil {
		t.Fatalf(
			"Recover: %v",
			err,
		)
	}
}

func TestProcessor_Recover_ReportsReadAndReplayFailures(
	t *testing.T,
) {
	ctx := context.Background()

	es := eventstore.New[order](
		&mockstores.ErrStore{Err: errors.New("log down")},
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	p := mustNew(
		t,
		es,
		bus.New[order](),
	)
	t.Cleanup(func() { p.Shutdown(ctx) })
	if err := p.Recover(
		ctx,
		10,
	); err == nil {
		t.Error("Recover succeeded with an unreadable delivery log")
	}
}

func TestProcessor_Recover_ParksAnAggregateWhoseEventCannotBeRebuilt(
	t *testing.T,
) {
	ctx := context.Background()
	s := store.New()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	seedPending(
		t,
		s,
		es,
		"good",
	)
	s.Append(
		ctx,
		"events:bad",
		1,
		[]byte("!!!"),
	)
	s.Atomically(
		ctx,
		func(
			tx asynxmd.Tx,
		) error {
			if err := tx.EnqueueDelivery(
				ctx,
				"bad",
				1,
			); err != nil {
				return err
			}

			return tx.EnqueueDelivery(
				ctx,
				"bad",
				2,
			)
		},
	)

	var got atomic.Int32
	reported := make(
		chan error,
		4,
	)
	p := mustNew(
		t,
		es,
		bus.New[order](),
		models.WithPublishErrorHandler(func(
			_ context.Context,
			e asynxmd.Event[order],
			err error,
		) {
			if e.AggregateID == "bad" {
				reported <- err
			}
		}),
		models.WithDurableSubscribers([]asynxmd.DurableSub[order]{{
			Match: always,
			Handler: func(
				context.Context,
				asynxmd.Event[order],
			) error {
				got.Add(1)
				return nil
			},
		}}),
	)
	t.Cleanup(func() { p.Shutdown(ctx) })

	if err := p.Recover(
		ctx,
		10,
	); err != nil {
		t.Fatalf(
			"Recover: %v",
			err,
		)
	}

	p.WaitPublish()
	if got.Load() != 1 {
		t.Errorf(
			"delivered %d events, want only the healthy aggregate's",
			got.Load(),
		)
	}

	select {
	case <-reported:
	case <-time.After(5 * time.Second):
		t.Fatal("the unreadable event was not reported")
	}

	if paused := p.Paused(); len(paused) != 1 ||
		paused[0] != "bad" {
		t.Fatalf(
			"Paused = %v, want [bad]",
			paused,
		)
	}

	if pending, _ := s.PendingDeliveries(
		ctx,
		10,
	); len(pending) != 2 {
		t.Fatalf(
			"pending = %v, want both of the bad aggregate's entries kept",
			pending,
		)
	}

	if err := p.Resume(
		ctx,
		10,
	); err == nil {
		t.Fatal("Resume hid the unreadable event")
	}

	if paused := p.Paused(); len(paused) != 1 {
		t.Fatalf(
			"Paused = %v after a failed Resume, want it still parked",
			paused,
		)
	}
}

func TestProcessor_Recover_ReportsAnUnrebuildableEventToThePublishErrorHandler(
	t *testing.T,
) {
	ctx := context.Background()
	s := store.New()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	seedUnrebuildable(
		t,
		s,
		"bad",
	)

	type report struct {
		event asynxmd.Event[order]
		err   error
	}
	reported := make(
		chan report,
		4,
	)
	p := mustNew(
		t,
		es,
		bus.New[order](),
		models.WithPublishErrorHandler(func(
			_ context.Context,
			e asynxmd.Event[order],
			err error,
		) {
			reported <- report{e, err}
		}),
		models.WithDurableSubscribers([]asynxmd.DurableSub[order]{{
			Match: always,
			Handler: func(
				context.Context,
				asynxmd.Event[order],
			) error {
				return nil
			},
		}}),
	)
	t.Cleanup(func() { p.Shutdown(ctx) })

	if err := p.Recover(
		ctx,
		10,
	); err != nil {
		t.Fatalf(
			"Recover: %v",
			err,
		)
	}

	var got report
	select {
	case got = <-reported:
	case <-time.After(5 * time.Second):
		t.Fatal("the unrebuildable event was not reported")
	}

	if got.event.AggregateID != "bad" ||
		got.event.Version != 1 {
		t.Errorf(
			"reported event = %s v%d, want bad v1",
			got.event.AggregateID,
			got.event.Version,
		)
	}

	wrongReport := got.err == nil || !strings.HasPrefix(
		got.err.Error(),
		"recovery: ",
	) || errors.Unwrap(got.err) == nil
	if wrongReport {
		t.Errorf(
			"reported error = %v, want one wrapping the recovery error",
			got.err,
		)
	}
}

func TestProcessor_Recover_APanickingPublishErrorHandlerDoesNotCrash(
	t *testing.T,
) {
	ctx := context.Background()
	s := store.New()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	seedPending(
		t,
		s,
		es,
		"a",
	)
	seedUnrebuildable(
		t,
		s,
		"bad",
	)
	seedPending(
		t,
		s,
		es,
		"z",
	)

	var called atomic.Int32
	var delivered sync.Map
	p := mustNew(
		t,
		es,
		bus.New[order](),
		models.WithPublishErrorHandler(func(
			context.Context,
			asynxmd.Event[order],
			error,
		) {
			called.Add(1)
			panic("publish error handler exploded")
		}),
		models.WithDurableSubscribers([]asynxmd.DurableSub[order]{{
			Match: always,
			Handler: func(
				_ context.Context,
				e asynxmd.Event[order],
			) error {
				delivered.Store(
					e.AggregateID,
					true,
				)
				return nil
			},
		}}),
	)
	t.Cleanup(func() { p.Shutdown(ctx) })

	if err := p.Recover(
		ctx,
		10,
	); err != nil {
		t.Fatalf(
			"Recover: %v",
			err,
		)
	}

	p.WaitPublish()
	if called.Load() == 0 {
		t.Fatal("the publish error handler was not called")
	}

	assertRecovered(
		t,
		&delivered,
		"a",
		"z",
	)
	if paused := p.Paused(); !slices.Equal(
		paused,
		[]string{"bad"},
	) {
		t.Errorf(
			"Paused = %v, want [bad]",
			paused,
		)
	}
}

func TestProcessor_Recover_ReturnsWithoutWaitingForHandlers(
	t *testing.T,
) {
	s := store.New()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	seedPending(
		t,
		s,
		es,
		"a",
	)

	b := bus.New[order]()
	release := make(chan struct{})
	b.Subscribe(
		"OrderCreated",
		func(
			context.Context,
			asynxmd.Event[order],
		) {
			<-release
		},
	)
	p := mustNew(
		t,
		es,
		b,
	)
	t.Cleanup(func() { close(release); p.Shutdown(context.Background()) })

	returned := make(
		chan error,
		1,
	)
	go func() {
		returned <- p.Recover(
			context.Background(),
			10,
		)
	}()
	select {
	case err := <-returned:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Recover waited for a blocked handler")
	}
}

func TestProcessor_Recover_ReportsAnAckFailureForAVanishedEvent(
	t *testing.T,
) {
	s := ackErrStore{store.New()}
	ctx := context.Background()
	s.Atomically(
		ctx,
		func(
			tx asynxmd.Tx,
		) error {
			return tx.EnqueueDelivery(
				ctx,
				"ghost",
				1,
			)
		},
	)
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	reported := make(
		chan error,
		1,
	)
	p := mustNew(
		t,
		es,
		bus.New[order](),
		models.WithPublishErrorHandler(func(
			_ context.Context,
			_ asynxmd.Event[order],
			err error,
		) {
			reported <- err
		}),
	)
	t.Cleanup(func() { p.Shutdown(ctx) })

	if err := p.Recover(
		ctx,
		10,
	); err != nil {
		t.Fatalf(
			"Recover: %v",
			err,
		)
	}

	select {
	case <-reported:
	case <-time.After(5 * time.Second):
		t.Fatal("Recover hid an ack failure")
	}
}

func TestProcessor_Recover_AfterShutdownFails(
	t *testing.T,
) {
	s := store.New()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	seedPending(
		t,
		s,
		es,
		"a",
	)
	p := mustNew(
		t,
		es,
		bus.New[order](),
	)
	if err := p.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	if err := p.Recover(
		context.Background(),
		10,
	); !errors.Is(
		err,
		asynxmd.ErrDispatcherClosed,
	) {
		t.Fatalf(
			"Recover on a closed dispatcher = %v, want ErrDispatcherClosed",
			err,
		)
	}
}

func TestProcessor_Recover_ReplaysAnAggregatesStreamOnce(
	t *testing.T,
) {
	const events = 300
	ctx := context.Background()
	s := &rowCountingStore{Memory: store.New()}
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	if _, err := es.Write(
		ctx,
		mocks.CreateOrderCmd{ID: "a", Total: 1},
	); err != nil {
		t.Fatal(err)
	}

	for i := 1; i < events; i++ {
		if _, err := es.Write(
			ctx,
			updateCmd(
				"a",
				float64(i),
			),
		); err != nil {
			t.Fatal(err)
		}
	}

	s.Atomically(
		ctx,
		func(
			tx asynxmd.Tx,
		) error {
			return enqueueUpTo(
				ctx,
				tx,
				"a",
				events,
			)
		},
	)

	var mu sync.Mutex
	var delivered []int64
	p := mustNew(
		t,
		es,
		bus.New[order](),
		models.WithDurableSubscribers([]asynxmd.DurableSub[order]{{
			Match: always,
			Handler: func(
				_ context.Context,
				e asynxmd.Event[order],
			) error {
				mu.Lock()
				defer mu.Unlock()
				delivered = append(
					delivered,
					e.Version,
				)
				return nil
			},
		}}),
	)
	t.Cleanup(func() { p.Shutdown(ctx) })

	s.rows.Store(0)
	if err := p.Recover(
		ctx,
		50,
	); err != nil {
		t.Fatalf(
			"Recover: %v",
			err,
		)
	}

	p.WaitPublish()

	mu.Lock()
	defer mu.Unlock()
	if len(delivered) != events ||
		!slices.IsSorted(delivered) {
		t.Fatalf(
			"delivered %d events (sorted: %v), want %d in order",
			len(delivered),
			slices.IsSorted(delivered),
			events,
		)
	}

	if rows := s.rows.Load(); rows > 2*events {
		t.Fatalf(
			"recovery read %d stream rows for %d pending entries, want a single pass",
			rows,
			events,
		)
	}
}

func TestProcessor_Recover_PartialRecoveryDoesNotReportDeliveredEventsAsParked(
	t *testing.T,
) {
	ctx := context.Background()
	s := &ackSignalStore{Memory: store.New(), acked: make(
		chan struct{},
		8,
	)}
	var healthy atomic.Bool
	parked := make(
		chan struct{},
		1,
	)
	first := newFixture(
		t,
		s,
		parkingOpts(
			&healthy,
			parkedSignal(parked),
		)...,
	)
	for i, id := range []string{"c-1", "c-2", "c-3"} {
		if _, err := first.p.Send(
			ctx,
			idemOrderUpdate{updateCmd(
				"o1",
				float64(i+1),
			), id},
		); err != nil {
			t.Fatal(err)
		}

		if i == 0 {
			<-parked
		}
	}

	if err := first.p.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	var upcasts atomic.Int32
	upcaster := func(
		_ context.Context,
		_ string,
		patches []byte,
	) ([]byte, error) {
		if upcasts.Add(1) != 3 {
			return patches,
				nil
		}

		for range 2 {
			select {
			case <-s.acked:
			case <-time.After(5 * time.Second):
			}
		}

		return nil,
			errors.New("event cannot be upcast")
	}
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		map[int]asynxmd.Upcaster{1: upcaster},
		2,
		nil,
	)
	reported := make(
		chan struct{},
		1,
	)
	p := mustNew(
		t,
		es,
		bus.New[order](),
		models.WithDurableDelivery[order](func(
			name string,
		) bool {
			return name == "OrderUpdated"
		}),
		models.WithPublishErrorHandler(func(
			context.Context,
			asynxmd.Event[order],
			error,
		) {
			reported <- struct{}{}
		}),
		models.WithDurableSubscribers([]asynxmd.DurableSub[order]{{
			Match: always,
			Handler: func(
				context.Context,
				asynxmd.Event[order],
			) error {
				return nil
			},
		}}),
	)
	t.Cleanup(func() { p.Shutdown(ctx) })

	if err := p.Recover(
		ctx,
		10,
	); err != nil {
		t.Fatalf(
			"Recover: %v",
			err,
		)
	}

	select {
	case <-reported:
	case <-time.After(5 * time.Second):
		t.Fatal("the failure was not reported")
	}

	if pending, _ := s.PendingDeliveries(
		ctx,
		10,
	); len(pending) != 1 ||
		pending[0].Version != 3 {
		t.Fatalf(
			"pending = %v, want only version 3",
			pending,
		)
	}

	_, delivered := p.SendWait(
		ctx,
		idemOrderUpdate{updateCmd(
			"o1",
			1,
		), "c-1"},
	)
	_, behind := p.SendWait(
		ctx,
		idemOrderUpdate{updateCmd(
			"o1",
			3,
		), "c-3"},
	)
	if delivered != nil ||
		!errors.Is(
			behind,
			asynxmd.ErrDeliveryParked,
		) {
		t.Fatalf(
			"retries = %v, %v; want nil for c-1 (delivered and acknowledged) and ErrDeliveryParked for c-3",
			delivered,
			behind,
		)
	}
}

func TestProcessor_Recover_DeliversWhatPrecedesAnUnreadableEvent(
	t *testing.T,
) {
	ctx := context.Background()
	s := store.New()
	es := eventstore.New[order](
		s,
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
	for _, cmd := range []asynxmd.Command[order]{mocks.CreateOrderCmd{ID: "a", Total: 1}, updateCmd(
		"a",
		2,
	)} {
		if _, err := es.Write(
			ctx,
			cmd,
		); err != nil {
			t.Fatal(err)
		}
	}

	s.Append(
		ctx,
		"events:a",
		3,
		[]byte("!!!"),
	)
	s.Atomically(
		ctx,
		func(
			tx asynxmd.Tx,
		) error {
			return errors.Join(
				tx.EnqueueDelivery(
					ctx,
					"a",
					1,
				),
				tx.EnqueueDelivery(
					ctx,
					"a",
					3,
				),
			)
		},
	)

	var got atomic.Int32
	reported := make(
		chan error,
		1,
	)
	p := mustNew(
		t,
		es,
		bus.New[order](),
		models.WithPublishErrorHandler(func(
			_ context.Context,
			_ asynxmd.Event[order],
			err error,
		) {
			reported <- err
		}),
		models.WithDurableSubscribers([]asynxmd.DurableSub[order]{{
			Match: always,
			Handler: func(
				context.Context,
				asynxmd.Event[order],
			) error {
				got.Add(1)
				return nil
			},
		}}),
	)
	t.Cleanup(func() { p.Shutdown(ctx) })

	if err := p.Recover(
		ctx,
		10,
	); err != nil {
		t.Fatalf(
			"Recover: %v",
			err,
		)
	}

	select {
	case <-reported:
	case <-time.After(5 * time.Second):
		t.Fatal("the unreadable event was not reported")
	}

	p.WaitPublish()
	pending, _ := s.PendingDeliveries(
		ctx,
		10,
	)
	versions := make(
		[]int64,
		len(pending),
	)
	for i, d := range pending {
		versions[i] = d.Version
	}

	if !slices.Contains(
		versions,
		3,
	) {
		t.Fatalf(
			"pending = %v, want version 3 (behind the unreadable event) kept",
			versions,
		)
	}

	delivered := got.Load() == 1
	if delivered == slices.Contains(
		versions,
		1,
	) {
		t.Fatalf(
			"delivered %d, pending %v: version 1 must be either delivered and acknowledged, or still pending",
			got.Load(),
			versions,
		)
	}
}

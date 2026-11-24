package asynx_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/char2cs/asynx"
	"github.com/char2cs/asynx/internal/mockbuses"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/models/event"
	"github.com/char2cs/asynx/models/failure"
	"github.com/char2cs/asynx/store"
	"sync"
	"testing"
	"time"
)

type eventStream = <-chan event.Event[mocks.Order]

func listenOrFatal(
	t *testing.T,
	instance asynx.Asynx[mocks.Order],
	pattern string,
	count int,
	label string,
) (eventStream, func()) {
	t.Helper()

	ch, unsub, err := instance.Listen(
		pattern,
		count,
	)
	if err != nil {
		t.Fatalf(
			"%sListen: %v",
			label,
			err,
		)
	}

	return ch,
		unsub
}

func sendWait(
	t *testing.T,
	instance asynx.Asynx[mocks.Order],
	cmd mocks.CreateOrderCmd,
	what string,
) {
	t.Helper()

	_, err := instance.SendWait(
		context.Background(),
		cmd,
	)
	if err != nil {
		t.Fatalf(
			"%s: %v",
			what,
			err,
		)
	}
}

func recv(
	t *testing.T,
	ch eventStream,
	timeoutMsg string,
) event.Event[mocks.Order] {
	t.Helper()

	select {
	case evt := <-ch:
		return evt
	case <-time.After(2 * time.Second):
		t.Fatal(timeoutMsg)
	}

	return event.Event[mocks.Order]{}
}

func recvOpen(
	t *testing.T,
	ch eventStream,
) {
	t.Helper()

	select {
	case _, ok := <-ch:
		requireTrue(
			t,
			ok,
			"channel closed before delivering event",
		)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func recvClosed(
	t *testing.T,
	ch eventStream,
) {
	t.Helper()

	select {
	case _, ok := <-ch:
		requireTrue(
			t,
			!ok,
			"expected channel to be closed after count events",
		)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for channel close")
	}
}

func requireTrue(
	t *testing.T,
	cond bool,
	msg string,
) {
	t.Helper()

	if !cond {
		t.Fatal(msg)
	}
}

func expectSilent(
	t *testing.T,
	ch eventStream,
	wait time.Duration,
	label string,
) {
	t.Helper()

	select {
	case evt, ok := <-ch:
		requireTrue(
			t,
			!ok,
			fmt.Sprintf(
				"%s: %v",
				label,
				evt,
			),
		)
	case <-time.After(wait):
	}
}

func expectEventID(
	t *testing.T,
	ch eventStream,
	expected string,
	idx int,
) {
	t.Helper()

	select {
	case evt := <-ch:
		requireTrue(
			t,
			evt.AggregateID == expected,
			fmt.Sprintf(
				"expected %s, got %s",
				expected,
				evt.AggregateID,
			),
		)
	case <-time.After(2 * time.Second):
		t.Fatalf(
			"timed out waiting for event %d",
			idx,
		)
	}
}

func sendWaitSeries(
	t *testing.T,
	instance asynx.Asynx[mocks.Order],
	prefix string,
	n int,
) {
	t.Helper()

	for i := range n {
		sendWait(
			t,
			instance,
			mocks.CreateOrderCmd{ID: fmt.Sprintf(
				"%s-%d",
				prefix,
				i,
			), Total: float64(i + 1)},
			"SendWait",
		)
	}
}

func sendOne(
	wg *sync.WaitGroup,
	instance asynx.Asynx[mocks.Order],
	id string,
	total float64,
) {
	defer wg.Done()
	instance.Send(
		context.Background(),
		mocks.CreateOrderCmd{ID: id, Total: total},
	)
}

func sendBurst(
	wg *sync.WaitGroup,
	instance asynx.Asynx[mocks.Order],
	prefix string,
	iter int,
	n int,
) {
	for i := range n {
		wg.Add(1)
		go sendOne(
			wg,
			instance,
			fmt.Sprintf(
				"%s-%d-%d",
				prefix,
				iter,
				i,
			),
			float64(i+1),
		)
	}
}

func drain(
	ch eventStream,
) {
	for range ch {
	}
}

func drainThenClose(
	ch eventStream,
	drained chan<- struct{},
) {
	drain(ch)
	close(drained)
}

func nextEvent(
	t *testing.T,
	ch eventStream,
	deadline <-chan time.Time,
	what string,
	got int,
) bool {
	t.Helper()

	select {
	case _, ok := <-ch:
		return ok
	case <-deadline:
		t.Fatalf(
			"%s: timed out: got %d events, channel not closed",
			what,
			got,
		)
	}

	return false
}

func TestListen_EmptyPattern(
	t *testing.T,
) {
	instance := newInstance(t)
	_, _, err := instance.Listen(
		"",
		1,
	)
	if err != failure.ErrEmptyPattern {
		t.Fatalf(
			"expected ErrEmptyPattern, got %v",
			err,
		)
	}
}

func TestListen_EventArrivesBeforeDeadline(
	t *testing.T,
) {
	instance := newInstance(t)

	ch, unsub := listenOrFatal(
		t,
		instance,
		"OrderCreated",
		1,
		"",
	)
	defer unsub()

	sendWait(
		t,
		instance,
		mocks.CreateOrderCmd{ID: "listen-1", Total: 50.0},
		"SendWait",
	)

	evt := recv(
		t,
		ch,
		"timed out waiting for event",
	)
	if evt.AggregateID != "listen-1" {
		t.Fatalf(
			"expected AggregateID listen-1, got %q",
			evt.AggregateID,
		)
	}
}

func TestListen_UnsubscribeCleansUp(
	t *testing.T,
) {
	instance := newInstance(t)

	ch, unsub := listenOrFatal(
		t,
		instance,
		"OrderCreated",
		0,
		"",
	)

	unsub()

	sendWait(
		t,
		instance,
		mocks.CreateOrderCmd{ID: "listen-unsub", Total: 50.0},
		"SendWait",
	)

	expectSilent(
		t,
		ch,
		200*time.Millisecond,
		"expected no event after unsub, got",
	)
}

func TestListen_UnsubIsIdempotent(
	t *testing.T,
) {
	instance := newInstance(t)

	_, unsub := listenOrFatal(
		t,
		instance,
		"OrderCreated",
		0,
		"",
	)

	unsub()
	unsub()
}

func TestListen_CountAutoCloses(
	t *testing.T,
) {
	instance := newInstance(t)

	ch, unsub := listenOrFatal(
		t,
		instance,
		"OrderCreated",
		1,
		"",
	)
	defer unsub()

	sendWait(
		t,
		instance,
		mocks.CreateOrderCmd{ID: "listen-auto", Total: 50.0},
		"SendWait",
	)

	recvOpen(
		t,
		ch,
	)
	recvClosed(
		t,
		ch,
	)
}

func raceOnce(
	t *testing.T,
	instance asynx.Asynx[mocks.Order],
	w int,
	n int,
) bool {
	ch, unsub, err := instance.Listen(
		"OrderCreated",
		1,
	)
	if err != nil {
		t.Errorf(
			"Listen: %v",
			err,
		)
		return false
	}

	id := fmt.Sprintf(
		"listen-race-%d-%d",
		w,
		n,
	)
	_, err = instance.Send(
		context.Background(),
		mocks.CreateOrderCmd{ID: id, Total: 10},
	)
	if err != nil {
		t.Errorf(
			"Send: %v",
			err,
		)
		unsub()
		return false
	}

	select {
	case <-ch:
		unsub()
		return true
	case <-time.After(5 * time.Second):
		t.Error("timed out waiting for event")
		unsub()
		return false
	}
}

func raceWorker(
	t *testing.T,
	wg *sync.WaitGroup,
	instance asynx.Asynx[mocks.Order],
	w int,
	iterations int,
) {
	defer wg.Done()

	ok := true
	for n := 0; ok && n < iterations; n++ {
		ok = raceOnce(
			t,
			instance,
			w,
			n,
		)
	}
}

func TestListen_UnsubRightAfterLastEventDoesNotDoubleClose(
	t *testing.T,
) {
	instance := newInstance(t)

	const workers, iterations = 8, 250
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go raceWorker(
			t,
			&wg,
			instance,
			w,
			iterations,
		)
	}

	wg.Wait()
}

func TestListen_BufferedChannelPreservesEvent(
	t *testing.T,
) {
	instance := newInstance(t)

	ch, unsub := listenOrFatal(
		t,
		instance,
		"OrderCreated",
		1,
		"",
	)
	defer unsub()

	sendWait(
		t,
		instance,
		mocks.CreateOrderCmd{ID: "listen-buf", Total: 75.0},
		"SendWait",
	)

	evt := recv(
		t,
		ch,
		"event was lost — not in buffer",
	)
	if evt.Aggregate.Total != 75.0 {
		t.Fatalf(
			"expected Total 75.0, got %f",
			evt.Aggregate.Total,
		)
	}
}

func TestListen_Unbounded_ReceivesMultipleEvents(
	t *testing.T,
) {
	instance := newInstance(t)

	ch, unsub := listenOrFatal(
		t,
		instance,
		"OrderCreated",
		0,
		"",
	)
	defer unsub()

	sendWaitSeries(
		t,
		instance,
		"listen-ub",
		3,
	)

	for i := range 3 {
		expectEventID(
			t,
			ch,
			fmt.Sprintf(
				"listen-ub-%d",
				i,
			),
			i,
		)
	}
}

func TestListen_UsesTopicPattern(
	t *testing.T,
) {
	instance := newInstance(t)

	ch, unsub := listenOrFatal(
		t,
		instance,
		"*",
		1,
		"",
	)
	defer unsub()

	sendWait(
		t,
		instance,
		mocks.CreateOrderCmd{ID: "listen-topic", Total: 10.0},
		"SendWait",
	)

	evt := recv(
		t,
		ch,
		"topic pattern did not match — Listen should use Topic() internally",
	)
	if evt.AggregateID != "listen-topic" {
		t.Fatalf(
			"expected listen-topic, got %q",
			evt.AggregateID,
		)
	}
}

func TestListen_CountOverflow_DoesNotBlock(
	t *testing.T,
) {
	instance := newInstance(t)

	ch, unsub, err := instance.Listen(
		"OrderCreated",
		1,
	)
	if err != nil {
		t.Fatalf(
			"Listen: %v",
			err,
		)
	}

	defer unsub()

	if _, err := instance.SendWait(
		context.Background(),
		mocks.CreateOrderCmd{ID: "ovf-1", Total: 1.0},
	); err != nil {
		t.Fatalf(
			"SendWait 1: %v",
			err,
		)
	}

	if _, err := instance.SendWait(
		context.Background(),
		mocks.CreateOrderCmd{ID: "ovf-2", Total: 2.0},
	); err != nil {
		t.Fatalf(
			"SendWait 2: %v",
			err,
		)
	}

	<-ch
	if _, ok := <-ch; ok {
		t.Fatal("expected channel closed")
	}
}

func overflowIter(
	t *testing.T,
	iter int,
) {
	instance := newInstance(t)

	const count = 1
	label := fmt.Sprintf(
		"iter %d: ",
		iter,
	)
	ch, unsub := listenOrFatal(
		t,
		instance,
		"OrderCreated",
		count,
		label,
	)
	defer unsub()

	var wg sync.WaitGroup
	sendBurst(
		&wg,
		instance,
		"ovfc",
		iter,
		16,
	)

	wg.Wait()
	waitPublish(
		t,
		instance,
	)

	got := 0
	deadline := time.After(2 * time.Second)
	for nextEvent(
		t,
		ch,
		deadline,
		fmt.Sprintf(
			"iter %d",
			iter,
		),
		got,
	) {
		got++
	}

	if got != count {
		t.Fatalf(
			"iter %d: expected %d events before close, got %d",
			iter,
			count,
			got,
		)
	}
}

func TestListen_CountOverflowConcurrent_GuardsEnforced(
	t *testing.T,
) {
	for iter := range 10 {
		overflowIter(
			t,
			iter,
		)
	}
}

func TestListen_SubscribeError_ReturnsError(
	t *testing.T,
) {
	s := store.New()
	instance, err := asynx.New[mocks.Order]().WithEventStore(s).WithSnapshotStore(store.NewSnapshots()).Build()
	if err != nil {
		t.Fatal(err)
	}

	instance.Shutdown(context.Background())

	ch, unsub, err := instance.Listen(
		"OrderCreated",
		1,
	)
	if err == nil {
		t.Fatal("expected error when bus is closed, got nil")
	}

	if ch != nil {
		t.Fatalf("expected nil channel on error, got non-nil")
	}

	if unsub != nil {
		t.Fatalf("expected nil unsub on error, got non-nil")
	}
}

func TestListen_NegativeCount_TreatedAsZero(
	t *testing.T,
) {
	instance := newInstance(t)
	ch, unsub, err := instance.Listen(
		"OrderCreated",
		-1,
	)
	if err != nil {
		t.Fatalf(
			"expected nil error for negative count, got %v",
			err,
		)
	}

	defer unsub()

	sendWaitSeries(
		t,
		instance,
		"neg",
		2,
	)

	for i := range 2 {
		expectEventID(
			t,
			ch,
			fmt.Sprintf(
				"neg-%d",
				i,
			),
			i,
		)
	}
}

func raceIter(
	t *testing.T,
	iter int,
	count int,
	sends int,
	prefix string,
) {
	instance := newInstance(t)

	ch, unsub := listenOrFatal(
		t,
		instance,
		"OrderCreated",
		count,
		fmt.Sprintf(
			"iter %d: ",
			iter,
		),
	)
	defer unsub()

	go drain(ch)

	var wg sync.WaitGroup
	sendBurst(
		&wg,
		instance,
		prefix,
		iter,
		sends,
	)

	wg.Wait()
	waitPublish(
		t,
		instance,
	)
}

func TestListen_BoundedRace_NOverCount(
	t *testing.T,
) {
	for iter := range 10 {
		raceIter(
			t,
			iter,
			1,
			100,
			"race1",
		)
	}
}

func TestListen_BoundedRace_PostClosedGuard(
	t *testing.T,
) {
	for iter := range 10 {
		raceIter(
			t,
			iter,
			5,
			50,
			"race2",
		)
	}
}

func unsubDuringHandlerIter(
	t *testing.T,
	iter int,
) {
	instance := newInstance(t)

	ch, unsub := listenOrFatal(
		t,
		instance,
		"OrderCreated",
		50,
		fmt.Sprintf(
			"iter %d: ",
			iter,
		),
	)

	drained := make(chan struct{})
	go drainThenClose(
		ch,
		drained,
	)

	var wg sync.WaitGroup
	sendBurst(
		&wg,
		instance,
		"uh",
		iter,
		20,
	)

	unsub()
	wg.Wait()

	<-drained
	instance.Shutdown(context.Background())
}

func TestListen_UnsubDuringHandler_BoundedReturnsCleanly(
	t *testing.T,
) {
	for iter := 0; iter < 100; iter++ {
		unsubDuringHandlerIter(
			t,
			iter,
		)
	}
}

func immediateFireIter(
	t *testing.T,
	iter int,
) {
	instance := newInstance(t)

	label := fmt.Sprintf(
		"iter %d: ",
		iter,
	)
	ch, unsub := listenOrFatal(
		t,
		instance,
		"OrderCreated",
		1,
		label,
	)

	sendWait(
		t,
		instance,
		mocks.CreateOrderCmd{ID: "leak-1", Total: 1.0},
		label+"SendWait",
	)

	recv(
		t,
		ch,
		label+"did not receive event",
	)

	sendWait(
		t,
		instance,
		mocks.CreateOrderCmd{ID: "leak-2", Total: 1.0},
		label+"SendWait 2",
	)

	expectSilent(
		t,
		ch,
		50*time.Millisecond,
		label+"leaked event on closed-channel sub",
	)

	unsub()
	instance.Shutdown(context.Background())
}

func TestListen_BoundedSubID_NoLeakOnImmediateFire(
	t *testing.T,
) {
	for iter := 0; iter < 50; iter++ {
		immediateFireIter(
			t,
			iter,
		)
	}
}

func unsubThenClose(
	unsub func(),
	done chan<- struct{},
) {
	unsub()
	close(done)
}

func take(
	ch eventStream,
	deadline <-chan time.Time,
) bool {
	select {
	case <-ch:
		return true
	case <-deadline:
		return false
	}
}

func fillBuffer(
	t *testing.T,
	instance asynx.Asynx[mocks.Order],
	n int,
) {
	t.Helper()

	for i := range n {
		sendFill(
			t,
			instance,
			i,
		)
	}
}

func sendFill(
	t *testing.T,
	instance asynx.Asynx[mocks.Order],
	i int,
) {
	t.Helper()

	_, err := instance.Send(
		context.Background(),
		mocks.CreateOrderCmd{ID: fmt.Sprintf(
			"fill-%d",
			i,
		), Total: 1.0},
	)
	if err != nil {
		t.Fatalf(
			"Send %d: %v",
			i,
			err,
		)
	}
}

func TestListen_UnboundedMode_UnsubReleasesBlockedHandler(
	t *testing.T,
) {
	instance := newInstance(t)

	ch, unsub := listenOrFatal(
		t,
		instance,
		"OrderCreated",
		0,
		"",
	)

	fillBuffer(
		t,
		instance,
		25,
	)

	eventually(
		t,
		"the Listen buffer to fill",
		func() bool { return len(ch) == cap(ch) },
	)

	done := make(chan struct{})
	go unsubThenClose(
		unsub,
		done,
	)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("unsub deadlocked — handlers blocked on full channel")
	}

	drainTimeout := time.After(500 * time.Millisecond)
	for take(
		ch,
		drainTimeout,
	) {
	}
}

func concurrentIter(
	t *testing.T,
	iter int,
) {
	instance := newInstance(t)

	const count = 5
	what := fmt.Sprintf(
		"iter %d",
		iter,
	)
	ch, unsub := listenOrFatal(
		t,
		instance,
		"OrderCreated",
		count,
		what+": ",
	)

	var wg sync.WaitGroup
	sendBurst(
		&wg,
		instance,
		"br",
		iter,
		count,
	)

	wg.Wait()

	received := 0
	deadline := time.After(5 * time.Second)
	for nextEvent(
		t,
		ch,
		deadline,
		what,
		received,
	) {
		received++
	}

	if received != count {
		t.Fatalf(
			"iter %d: received %d events, want %d (close-race bug?)",
			iter,
			received,
			count,
		)
	}

	unsub()
	instance.Shutdown(context.Background())
}

func TestListen_BoundedConcurrent_DeliversAllCountEvents(
	t *testing.T,
) {
	for iter := 0; iter < 20; iter++ {
		concurrentIter(
			t,
			iter,
		)
	}
}

type refusingBus struct {
	mockbuses.Bus[mocks.Order]
	err error
}

// Unsubscribe refuses every removal with the configured error.
func (b *refusingBus) Unsubscribe(
	_ string,
) error {
	return b.err
}

type errorLog struct {
	all []error
}

func (l *errorLog) add(
	err error,
) {
	l.all = append(
		l.all,
		err,
	)
}

func buildWithRefusingBus(
	t *testing.T,
	refusal error,
	onError func(error),
) asynx.Asynx[mocks.Order] {
	t.Helper()

	instance, err := asynx.New[mocks.Order]().
		WithEventStore(store.New()).
		WithSnapshotStore(store.NewSnapshots()).
		WithBus(&refusingBus{err: refusal}).
		WithListenerErrorHandler(onError).
		Build()
	if err != nil {
		t.Fatalf(
			"Build: %v",
			err,
		)
	}

	return instance
}

func TestListen_RefusedUnsubscribeIsReportedToTheListenerErrorHandler(
	t *testing.T,
) {
	refusal := errors.New("bus refused")
	var errs errorLog
	instance := buildWithRefusingBus(
		t,
		refusal,
		errs.add,
	)

	_, unsub, err := instance.Listen(
		"OrderCreated",
		0,
	)
	if err != nil {
		t.Fatalf(
			"Listen: %v",
			err,
		)
	}

	unsub()
	unsub()

	reported := errs.all
	if len(reported) != 1 {
		t.Fatalf(
			"reported %d errors, want 1 (unsub is idempotent)",
			len(reported),
		)
	}

	if !errors.Is(
		reported[0],
		refusal,
	) {
		t.Errorf(
			"reported = %v, want it to wrap the bus error",
			reported[0],
		)
	}
}

func TestSubscribeWait_RefusedUnsubscribeIsJoinedToTheResult(
	t *testing.T,
) {
	refusal := errors.New("bus refused")
	instance := buildWithRefusingBus(
		t,
		refusal,
		nil,
	)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := instance.SubscribeWait(
		ctx,
		"OrderCreated",
	)
	if !errors.Is(
		err,
		context.Canceled,
	) ||
		!errors.Is(
			err,
			refusal,
		) {
		t.Errorf(
			"SubscribeWait = %v, want both the context error and the bus error",
			err,
		)
	}
}

package asynx_test

import (
	"context"
	"fmt"
	"github.com/char2cs/asynx"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
	"sync"
	"testing"
	"time"
)

type lstEvents = <-chan models.Event[mocks.Order]

func lstListen(
	t *testing.T,
	instance asynx.Asynx[mocks.Order],
	pattern string,
	count int,
	label string,
) (lstEvents, func()) {
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

func lstSendWait(
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

func lstRecv(
	t *testing.T,
	ch lstEvents,
	timeoutMsg string,
) models.Event[mocks.Order] {
	t.Helper()

	select {
	case evt := <-ch:
		return evt
	case <-time.After(2 * time.Second):
		t.Fatal(timeoutMsg)
	}

	return models.Event[mocks.Order]{}
}

func lstRecvOpen(
	t *testing.T,
	ch lstEvents,
) {
	t.Helper()

	select {
	case _, ok := <-ch:
		lstRequireTrue(
			t,
			ok,
			"channel closed before delivering event",
		)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func lstRecvClosed(
	t *testing.T,
	ch lstEvents,
) {
	t.Helper()

	select {
	case _, ok := <-ch:
		lstRequireTrue(
			t,
			!ok,
			"expected channel to be closed after count events",
		)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for channel close")
	}
}

func lstRequireTrue(
	t *testing.T,
	cond bool,
	msg string,
) {
	t.Helper()

	if !cond {
		t.Fatal(msg)
	}
}

func lstExpectSilent(
	t *testing.T,
	ch lstEvents,
	wait time.Duration,
	label string,
) {
	t.Helper()

	select {
	case evt, ok := <-ch:
		lstRequireTrue(
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

func lstExpectEventID(
	t *testing.T,
	ch lstEvents,
	expected string,
	idx int,
) {
	t.Helper()

	select {
	case evt := <-ch:
		lstRequireTrue(
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

func lstSendWaitSeries(
	t *testing.T,
	instance asynx.Asynx[mocks.Order],
	prefix string,
	n int,
) {
	t.Helper()

	for i := range n {
		lstSendWait(
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

func lstSendOne(
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

func lstSendBurst(
	wg *sync.WaitGroup,
	instance asynx.Asynx[mocks.Order],
	prefix string,
	iter int,
	n int,
) {
	for i := range n {
		wg.Add(1)
		go lstSendOne(
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

func lstDrain(
	ch lstEvents,
) {
	for range ch {
	}
}

func lstDrainThenClose(
	ch lstEvents,
	drained chan<- struct{},
) {
	lstDrain(ch)
	close(drained)
}

func lstNext(
	t *testing.T,
	ch lstEvents,
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
	if err != models.ErrEmptyPattern {
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

	ch, unsub := lstListen(
		t,
		instance,
		"OrderCreated",
		1,
		"",
	)
	defer unsub()

	lstSendWait(
		t,
		instance,
		mocks.CreateOrderCmd{ID: "listen-1", Total: 50.0},
		"SendWait",
	)

	evt := lstRecv(
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

	ch, unsub := lstListen(
		t,
		instance,
		"OrderCreated",
		0,
		"",
	)

	unsub()

	lstSendWait(
		t,
		instance,
		mocks.CreateOrderCmd{ID: "listen-unsub", Total: 50.0},
		"SendWait",
	)

	lstExpectSilent(
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

	_, unsub := lstListen(
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

	ch, unsub := lstListen(
		t,
		instance,
		"OrderCreated",
		1,
		"",
	)
	defer unsub()

	lstSendWait(
		t,
		instance,
		mocks.CreateOrderCmd{ID: "listen-auto", Total: 50.0},
		"SendWait",
	)

	lstRecvOpen(
		t,
		ch,
	)
	lstRecvClosed(
		t,
		ch,
	)
}

func lstRaceOnce(
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

func lstRaceWorker(
	t *testing.T,
	wg *sync.WaitGroup,
	instance asynx.Asynx[mocks.Order],
	w int,
	iterations int,
) {
	defer wg.Done()

	ok := true
	for n := 0; ok && n < iterations; n++ {
		ok = lstRaceOnce(
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
		go lstRaceWorker(
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

	ch, unsub := lstListen(
		t,
		instance,
		"OrderCreated",
		1,
		"",
	)
	defer unsub()

	lstSendWait(
		t,
		instance,
		mocks.CreateOrderCmd{ID: "listen-buf", Total: 75.0},
		"SendWait",
	)

	evt := lstRecv(
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

	ch, unsub := lstListen(
		t,
		instance,
		"OrderCreated",
		0,
		"",
	)
	defer unsub()

	lstSendWaitSeries(
		t,
		instance,
		"listen-ub",
		3,
	)

	for i := range 3 {
		lstExpectEventID(
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

	ch, unsub := lstListen(
		t,
		instance,
		"*",
		1,
		"",
	)
	defer unsub()

	lstSendWait(
		t,
		instance,
		mocks.CreateOrderCmd{ID: "listen-topic", Total: 10.0},
		"SendWait",
	)

	evt := lstRecv(
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

func lstOverflowIter(
	t *testing.T,
	iter int,
) {
	instance := newInstance(t)

	const count = 1
	label := fmt.Sprintf(
		"iter %d: ",
		iter,
	)
	ch, unsub := lstListen(
		t,
		instance,
		"OrderCreated",
		count,
		label,
	)
	defer unsub()

	var wg sync.WaitGroup
	lstSendBurst(
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
	for lstNext(
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
		lstOverflowIter(
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

	lstSendWaitSeries(
		t,
		instance,
		"neg",
		2,
	)

	for i := range 2 {
		lstExpectEventID(
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

func lstRaceIter(
	t *testing.T,
	iter int,
	count int,
	sends int,
	prefix string,
) {
	instance := newInstance(t)

	ch, unsub := lstListen(
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

	go lstDrain(ch)

	var wg sync.WaitGroup
	lstSendBurst(
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
		lstRaceIter(
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
		lstRaceIter(
			t,
			iter,
			5,
			50,
			"race2",
		)
	}
}

func lstUnsubDuringHandlerIter(
	t *testing.T,
	iter int,
) {
	instance := newInstance(t)

	ch, unsub := lstListen(
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
	go lstDrainThenClose(
		ch,
		drained,
	)

	var wg sync.WaitGroup
	lstSendBurst(
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
		lstUnsubDuringHandlerIter(
			t,
			iter,
		)
	}
}

func lstImmediateFireIter(
	t *testing.T,
	iter int,
) {
	instance := newInstance(t)

	label := fmt.Sprintf(
		"iter %d: ",
		iter,
	)
	ch, unsub := lstListen(
		t,
		instance,
		"OrderCreated",
		1,
		label,
	)

	lstSendWait(
		t,
		instance,
		mocks.CreateOrderCmd{ID: "leak-1", Total: 1.0},
		label+"SendWait",
	)

	lstRecv(
		t,
		ch,
		label+"did not receive event",
	)

	lstSendWait(
		t,
		instance,
		mocks.CreateOrderCmd{ID: "leak-2", Total: 1.0},
		label+"SendWait 2",
	)

	lstExpectSilent(
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
		lstImmediateFireIter(
			t,
			iter,
		)
	}
}

func lstUnsubThenClose(
	unsub func(),
	done chan<- struct{},
) {
	unsub()
	close(done)
}

func lstTake(
	ch lstEvents,
	deadline <-chan time.Time,
) bool {
	select {
	case <-ch:
		return true
	case <-deadline:
		return false
	}
}

func lstFill(
	t *testing.T,
	instance asynx.Asynx[mocks.Order],
	n int,
) {
	t.Helper()

	for i := range n {
		lstSendFill(
			t,
			instance,
			i,
		)
	}
}

func lstSendFill(
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

	ch, unsub := lstListen(
		t,
		instance,
		"OrderCreated",
		0,
		"",
	)

	lstFill(
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
	go lstUnsubThenClose(
		unsub,
		done,
	)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("unsub deadlocked — handlers blocked on full channel")
	}

	drainTimeout := time.After(500 * time.Millisecond)
	for lstTake(
		ch,
		drainTimeout,
	) {
	}
}

func lstConcurrentIter(
	t *testing.T,
	iter int,
) {
	instance := newInstance(t)

	const count = 5
	what := fmt.Sprintf(
		"iter %d",
		iter,
	)
	ch, unsub := lstListen(
		t,
		instance,
		"OrderCreated",
		count,
		what+": ",
	)

	var wg sync.WaitGroup
	lstSendBurst(
		&wg,
		instance,
		"br",
		iter,
		count,
	)

	wg.Wait()

	received := 0
	deadline := time.After(5 * time.Second)
	for lstNext(
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
		lstConcurrentIter(
			t,
			iter,
		)
	}
}

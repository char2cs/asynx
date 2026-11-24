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

	cmd := mocks.CreateOrderCmd{ID: "listen-1", Total: 50.0}
	if _, err := instance.SendWait(
		context.Background(),
		cmd,
	); err != nil {
		t.Fatalf(
			"SendWait: %v",
			err,
		)
	}

	select {
	case evt := <-ch:
		if evt.AggregateID != "listen-1" {
			t.Fatalf(
				"expected AggregateID listen-1, got %q",
				evt.AggregateID,
			)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func TestListen_UnsubscribeCleansUp(
	t *testing.T,
) {
	instance := newInstance(t)

	ch, unsub, err := instance.Listen(
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

	cmd := mocks.CreateOrderCmd{ID: "listen-unsub", Total: 50.0}
	if _, err := instance.SendWait(
		context.Background(),
		cmd,
	); err != nil {
		t.Fatalf(
			"SendWait: %v",
			err,
		)
	}

	select {
	case evt, ok := <-ch:
		if ok {
			t.Fatalf(
				"expected no event after unsub, got %v",
				evt,
			)
		}
	case <-time.After(200 * time.Millisecond):
	}
}

func TestListen_UnsubIsIdempotent(
	t *testing.T,
) {
	instance := newInstance(t)

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
}

func TestListen_CountAutoCloses(
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

	cmd := mocks.CreateOrderCmd{ID: "listen-auto", Total: 50.0}
	if _, err := instance.SendWait(
		context.Background(),
		cmd,
	); err != nil {
		t.Fatalf(
			"SendWait: %v",
			err,
		)
	}

	select {
	case _, ok := <-ch:
		if !ok {
			t.Fatal("channel closed before delivering event")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
	}

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel to be closed after count events")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for channel close")
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
		go func(
			w int,
		) {
			defer wg.Done()
			for n := 0; n < iterations; n++ {
				ch, unsub, err := instance.Listen(
					"OrderCreated",
					1,
				)
				if err != nil {
					t.Errorf(
						"Listen: %v",
						err,
					)
					return
				}

				id := fmt.Sprintf(
					"listen-race-%d-%d",
					w,
					n,
				)
				if _, err := instance.Send(
					context.Background(),
					mocks.CreateOrderCmd{ID: id, Total: 10},
				); err != nil {
					t.Errorf(
						"Send: %v",
						err,
					)
					unsub()
					return
				}

				select {
				case <-ch:
					unsub()
				case <-time.After(5 * time.Second):
					t.Error("timed out waiting for event")
					unsub()
					return
				}
			}
		}(w)
	}

	wg.Wait()
}

func TestListen_BufferedChannelPreservesEvent(
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

	cmd := mocks.CreateOrderCmd{ID: "listen-buf", Total: 75.0}
	if _, err := instance.SendWait(
		context.Background(),
		cmd,
	); err != nil {
		t.Fatalf(
			"SendWait: %v",
			err,
		)
	}

	select {
	case evt := <-ch:
		if evt.Aggregate.Total != 75.0 {
			t.Fatalf(
				"expected Total 75.0, got %f",
				evt.Aggregate.Total,
			)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("event was lost — not in buffer")
	}
}

func TestListen_Unbounded_ReceivesMultipleEvents(
	t *testing.T,
) {
	instance := newInstance(t)

	ch, unsub, err := instance.Listen(
		"OrderCreated",
		0,
	)
	if err != nil {
		t.Fatalf(
			"Listen: %v",
			err,
		)
	}

	defer unsub()

	for i := range 3 {
		cmd := mocks.CreateOrderCmd{ID: fmt.Sprintf(
			"listen-ub-%d",
			i,
		), Total: float64(i + 1)}
		if _, err := instance.SendWait(
			context.Background(),
			cmd,
		); err != nil {
			t.Fatalf(
				"SendWait: %v",
				err,
			)
		}
	}

	for i := range 3 {
		select {
		case evt := <-ch:
			expected := fmt.Sprintf(
				"listen-ub-%d",
				i,
			)
			if evt.AggregateID != expected {
				t.Fatalf(
					"expected %s, got %s",
					expected,
					evt.AggregateID,
				)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf(
				"timed out waiting for event %d",
				i,
			)
		}
	}
}

func TestListen_UsesTopicPattern(
	t *testing.T,
) {
	instance := newInstance(t)

	ch, unsub, err := instance.Listen(
		"*",
		1,
	)
	if err != nil {
		t.Fatalf(
			"Listen: %v",
			err,
		)
	}

	defer unsub()

	cmd := mocks.CreateOrderCmd{ID: "listen-topic", Total: 10.0}
	if _, err := instance.SendWait(
		context.Background(),
		cmd,
	); err != nil {
		t.Fatalf(
			"SendWait: %v",
			err,
		)
	}

	select {
	case evt := <-ch:
		if evt.AggregateID != "listen-topic" {
			t.Fatalf(
				"expected listen-topic, got %q",
				evt.AggregateID,
			)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("topic pattern did not match — Listen should use Topic() internally")
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

func TestListen_CountOverflowConcurrent_GuardsEnforced(
	t *testing.T,
) {
	for iter := range 10 {
		func() {
			instance := newInstance(t)

			const count = 1
			ch, unsub, err := instance.Listen(
				"OrderCreated",
				count,
			)
			if err != nil {
				t.Fatalf(
					"iter %d: Listen: %v",
					iter,
					err,
				)
			}

			defer unsub()

			const total = 16
			var wg sync.WaitGroup
			for i := range total {
				wg.Add(1)
				go func(
					idx int,
				) {
					defer wg.Done()
					instance.Send(
						context.Background(),
						mocks.CreateOrderCmd{
							ID: fmt.Sprintf(
								"ovfc-%d-%d",
								iter,
								idx,
							),
							Total: float64(idx + 1),
						},
					)
				}(i)
			}

			wg.Wait()
			waitPublish(
				t,
				instance,
			)

			got := 0
			deadline := time.After(2 * time.Second)
			for {
				select {
				case _, ok := <-ch:
					if !ok {
						if got != count {
							t.Fatalf(
								"iter %d: expected %d events before close, got %d",
								iter,
								count,
								got,
							)
						}

						return
					}

					got++
				case <-deadline:
					t.Fatalf(
						"iter %d: timed out: got %d events, channel not closed",
						iter,
						got,
					)
				}
			}
		}()
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

	for i := range 2 {
		cmd := mocks.CreateOrderCmd{ID: fmt.Sprintf(
			"neg-%d",
			i,
		), Total: float64(i + 1)}
		if _, err := instance.SendWait(
			context.Background(),
			cmd,
		); err != nil {
			t.Fatalf(
				"SendWait: %v",
				err,
			)
		}
	}

	for i := range 2 {
		select {
		case evt := <-ch:
			expected := fmt.Sprintf(
				"neg-%d",
				i,
			)
			if evt.AggregateID != expected {
				t.Fatalf(
					"expected %s, got %s",
					expected,
					evt.AggregateID,
				)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf(
				"timed out waiting for event %d",
				i,
			)
		}
	}
}

func TestListen_BoundedRace_NOverCount(
	t *testing.T,
) {
	for iter := range 10 {
		func() {
			instance := newInstance(t)

			const count = 1
			ch, unsub, err := instance.Listen(
				"OrderCreated",
				count,
			)
			if err != nil {
				t.Fatalf(
					"iter %d: Listen: %v",
					iter,
					err,
				)
			}

			defer unsub()

			go func() {
				for range ch {
				}
			}()

			var wg sync.WaitGroup
			for i := range 100 {
				wg.Add(1)
				go func(
					idx int,
				) {
					defer wg.Done()
					instance.Send(
						context.Background(),
						mocks.CreateOrderCmd{
							ID: fmt.Sprintf(
								"race1-%d-%d",
								iter,
								idx,
							),
							Total: float64(idx + 1),
						},
					)
				}(i)
			}

			wg.Wait()
			waitPublish(
				t,
				instance,
			)
		}()
	}
}

func TestListen_BoundedRace_PostClosedGuard(
	t *testing.T,
) {
	for iter := range 10 {
		func() {
			instance := newInstance(t)

			const count = 5
			ch, unsub, err := instance.Listen(
				"OrderCreated",
				count,
			)
			if err != nil {
				t.Fatalf(
					"iter %d: Listen: %v",
					iter,
					err,
				)
			}

			defer unsub()

			go func() {
				for range ch {
				}
			}()

			var wg sync.WaitGroup
			for i := range 50 {
				wg.Add(1)
				go func(
					idx int,
				) {
					defer wg.Done()
					instance.Send(
						context.Background(),
						mocks.CreateOrderCmd{
							ID: fmt.Sprintf(
								"race2-%d-%d",
								iter,
								idx,
							),
							Total: float64(idx + 1),
						},
					)
				}(i)
			}

			wg.Wait()
			waitPublish(
				t,
				instance,
			)
		}()
	}
}

func TestListen_UnsubDuringHandler_BoundedReturnsCleanly(
	t *testing.T,
) {
	for iter := 0; iter < 100; iter++ {
		instance := newInstance(t)

		ch, unsub, err := instance.Listen(
			"OrderCreated",
			50,
		)
		if err != nil {
			t.Fatalf(
				"iter %d: Listen: %v",
				iter,
				err,
			)
		}

		drained := make(chan struct{})
		go func() {
			for range ch {
			}

			close(drained)
		}()

		var wg sync.WaitGroup
		for j := 0; j < 20; j++ {
			wg.Add(1)
			go func(
				idx int,
			) {
				defer wg.Done()
				cmd := mocks.CreateOrderCmd{ID: fmt.Sprintf(
					"uh-%d-%d",
					iter,
					idx,
				), Total: 1.0}
				_, _ = instance.Send(
					context.Background(),
					cmd,
				)
			}(j)
		}

		unsub()
		wg.Wait()

		<-drained
		instance.Shutdown(context.Background())
	}
}

func TestListen_BoundedSubID_NoLeakOnImmediateFire(
	t *testing.T,
) {
	for iter := 0; iter < 50; iter++ {
		instance := newInstance(t)

		ch, unsub, err := instance.Listen(
			"OrderCreated",
			1,
		)
		if err != nil {
			t.Fatalf(
				"iter %d: Listen: %v",
				iter,
				err,
			)
		}

		if _, err := instance.SendWait(
			context.Background(),
			mocks.CreateOrderCmd{ID: "leak-1", Total: 1.0},
		); err != nil {
			t.Fatalf(
				"iter %d: SendWait: %v",
				iter,
				err,
			)
		}

		select {
		case <-ch:
		case <-time.After(2 * time.Second):
			t.Fatalf(
				"iter %d: did not receive event",
				iter,
			)
		}

		if _, err := instance.SendWait(
			context.Background(),
			mocks.CreateOrderCmd{ID: "leak-2", Total: 1.0},
		); err != nil {
			t.Fatalf(
				"iter %d: SendWait 2: %v",
				iter,
				err,
			)
		}

		select {
		case evt, ok := <-ch:
			if ok {
				t.Fatalf(
					"iter %d: leaked event %v on closed-channel sub",
					iter,
					evt,
				)
			}
		case <-time.After(50 * time.Millisecond):
		}

		unsub()
		instance.Shutdown(context.Background())
	}
}

func TestListen_UnboundedMode_UnsubReleasesBlockedHandler(
	t *testing.T,
) {
	instance := newInstance(t)

	ch, unsub, err := instance.Listen(
		"OrderCreated",
		0,
	)
	if err != nil {
		t.Fatalf(
			"Listen: %v",
			err,
		)
	}

	for i := range 25 {
		cmd := mocks.CreateOrderCmd{ID: fmt.Sprintf(
			"fill-%d",
			i,
		), Total: 1.0}
		if _, err := instance.Send(
			context.Background(),
			cmd,
		); err != nil {
			t.Fatalf(
				"Send %d: %v",
				i,
				err,
			)
		}
	}

	eventually(
		t,
		"the Listen buffer to fill",
		func() bool { return len(ch) == cap(ch) },
	)

	done := make(chan struct{})
	go func() {
		unsub()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("unsub deadlocked — handlers blocked on full channel")
	}

	drainTimeout := time.After(500 * time.Millisecond)
	for {
		select {
		case <-ch:
		case <-drainTimeout:
			return
		}
	}
}

func TestListen_BoundedConcurrent_DeliversAllCountEvents(
	t *testing.T,
) {
	for iter := 0; iter < 20; iter++ {
		instance := newInstance(t)

		const count = 5
		ch, unsub, err := instance.Listen(
			"OrderCreated",
			count,
		)
		if err != nil {
			t.Fatalf(
				"iter %d: Listen: %v",
				iter,
				err,
			)
		}

		var wg sync.WaitGroup
		for j := 0; j < count; j++ {
			wg.Add(1)
			go func(
				idx int,
			) {
				defer wg.Done()
				cmd := mocks.CreateOrderCmd{ID: fmt.Sprintf(
					"br-%d-%d",
					iter,
					idx,
				), Total: float64(idx + 1)}
				_, _ = instance.Send(
					context.Background(),
					cmd,
				)
			}(j)
		}

		wg.Wait()

		received := 0
		drainCtx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		for {
			select {
			case _, ok := <-ch:
				if !ok {
					goto done
				}

				received++
			case <-drainCtx.Done():
				cancel()
				t.Fatalf(
					"iter %d: timed out waiting for events; received %d/%d",
					iter,
					received,
					count,
				)
			}
		}

	done:
		cancel()

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
}

package bus_test

import (
	"context"
	"testing"
	"time"

	"github.com/char2cs/asynx/internal/bus"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/models/event"
	"github.com/char2cs/asynx/models/handler"
	"github.com/char2cs/asynx/models/subscription"
)

func TestIsExactPattern_DistinguishesLiteralsFromRegexps(
	t *testing.T,
) {
	if !bus.IsExactPattern("OrderPlaced") {
		t.Fatal("OrderPlaced must be an exact pattern")
	}

	if bus.IsExactPattern("^Order.*") {
		t.Fatal("^Order.* must not be an exact pattern")
	}
}

func forwardsPanicValue(
	panicked chan<- any,
) handler.Panic[mocks.Order] {
	return func(
		_ context.Context,
		_ event.Event[mocks.Order],
		value any,
	) {
		panicked <- value
	}
}

func signalsTimeout(
	timedOut chan<- struct{},
) handler.Timeout[mocks.Order] {
	return func(
		_ context.Context,
		_ event.Event[mocks.Order],
		_ time.Duration,
	) {
		timedOut <- struct{}{}
	}
}

func panicsWith(
	value any,
) handler.Projection[mocks.Order] {
	return func(
		context.Context,
		event.Event[mocks.Order],
	) {
		panic(value)
	}
}

func sleepsFor(
	d time.Duration,
) handler.Projection[mocks.Order] {
	return func(
		context.Context,
		event.Event[mocks.Order],
	) {
		time.Sleep(d)
	}
}

func TestNew_WithHandlers_RoutesPanicAndTimeoutToThem(
	t *testing.T,
) {
	panicked := make(
		chan any,
		1,
	)
	timedOut := make(
		chan struct{},
		1,
	)
	b := bus.New[mocks.Order](
		bus.WithPanicHandler[mocks.Order](forwardsPanicValue(panicked)),
		bus.WithTimeoutHandler[mocks.Order](signalsTimeout(timedOut)),
	)
	defer b.Close(context.Background())
	_, err := b.Subscribe(
		"Boom",
		panicsWith("boom"),
	)
	if err != nil {
		t.Fatalf(
			"Subscribe: %v",
			err,
		)
	}

	_, err = b.Subscribe(
		"Slow",
		sleepsFor(50*time.Millisecond),
		subscription.WithHandlerTimeout[mocks.Order](5*time.Millisecond),
	)
	if err != nil {
		t.Fatalf(
			"Subscribe: %v",
			err,
		)
	}

	for _, name := range []string{"Boom", "Slow"} {
		publishSyncNamed(
			t,
			b,
			name,
		)
	}

	select {
	case <-panicked:
	case <-time.After(time.Second):
		t.Fatal("panic handler not called")
	}

	select {
	case <-timedOut:
	case <-time.After(time.Second):
		t.Fatal("timeout handler not called")
	}
}

func publishSyncNamed(
	t *testing.T,
	b bus.Bus[mocks.Order],
	name string,
) {
	t.Helper()

	err := b.PublishSync(
		context.Background(),
		event.Event[mocks.Order]{EventName: name},
	)
	if err != nil {
		t.Fatalf(
			"PublishSync %s: %v",
			name,
			err,
		)
	}
}

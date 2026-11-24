package bus_test

import (
	"context"
	"testing"
	"time"

	"github.com/char2cs/asynx/internal/bus"
	"github.com/char2cs/asynx/internal/mocks"
	asynxmd "github.com/char2cs/asynx/models"
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
	withPanicHandlerFn := func(
		_ context.Context,
		_ asynxmd.Event[mocks.Order],
		v any,
	) {
		panicked <- v
	}
	withTimeoutHandlerFn := func(
		_ context.Context,
		_ asynxmd.Event[mocks.Order],
		_ time.Duration,
	) {
		timedOut <- struct{}{}
	}
	b := bus.New[mocks.Order](
		bus.WithPanicHandler[mocks.Order](withPanicHandlerFn),
		bus.WithTimeoutHandler[mocks.Order](withTimeoutHandlerFn),
	)
	defer b.Close(context.Background())
	subscribeFn := func(
		context.Context,
		asynxmd.Event[mocks.Order],
	) {
		panic("boom")
	}
	_, err := b.Subscribe(
		"Boom",
		subscribeFn,
	)
	if err != nil {
		t.Fatalf(
			"Subscribe: %v",
			err,
		)
	}

	subscribeFn2 := func(
		context.Context,
		asynxmd.Event[mocks.Order],
	) {
		time.Sleep(50 * time.Millisecond)
	}
	_, err = b.Subscribe(
		"Slow",
		subscribeFn2,
		asynxmd.WithHandlerTimeout[mocks.Order](5*time.Millisecond),
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
		asynxmd.Event[mocks.Order]{EventName: name},
	)
	if err != nil {
		t.Fatalf(
			"PublishSync %s: %v",
			name,
			err,
		)
	}
}

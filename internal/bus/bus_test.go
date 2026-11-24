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
	b := bus.New[mocks.Order](
		bus.WithPanicHandler[mocks.Order](func(
			_ context.Context,
			_ asynxmd.Event[mocks.Order],
			v any,
		) {
			panicked <- v
		}),
		bus.WithTimeoutHandler[mocks.Order](func(
			_ context.Context,
			_ asynxmd.Event[mocks.Order],
			_ time.Duration,
		) {
			timedOut <- struct{}{}
		}),
	)
	defer b.Close(context.Background())

	_, err := b.Subscribe(
		"Boom",
		func(
			context.Context,
			asynxmd.Event[mocks.Order],
		) {
			panic("boom")
		},
	)
	if err != nil {
		t.Fatalf(
			"Subscribe: %v",
			err,
		)
	}

	_, err = b.Subscribe(
		"Slow",
		func(
			context.Context,
			asynxmd.Event[mocks.Order],
		) {
			time.Sleep(50 * time.Millisecond)
		},
		asynxmd.WithHandlerTimeout[mocks.Order](5*time.Millisecond),
	)
	if err != nil {
		t.Fatalf(
			"Subscribe: %v",
			err,
		)
	}

	for _, name := range []string{"Boom", "Slow"} {
		err = b.PublishSync(
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

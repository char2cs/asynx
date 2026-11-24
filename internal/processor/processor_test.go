package processor_test

import (
	"context"
	"errors"
	"testing"

	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/processor"
	"github.com/char2cs/asynx/internal/processor/models"
	asynxmd "github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
)

func newStore() eventstore.EventStore[mocks.Order] {
	return eventstore.New[mocks.Order](
		store.New(),
		store.NewSnapshots(),
		nil,
		1,
		nil,
	)
}

func TestNew_SendsACommandThroughTheProcessor(
	t *testing.T,
) {
	p, err := processor.New[mocks.Order](
		newStore(),
		nil,
		models.WithShards[mocks.Order](2),
	)
	if err != nil {
		t.Fatalf(
			"New: %v",
			err,
		)
	}

	defer p.Shutdown(context.Background())

	event, err := p.Send(
		context.Background(),
		mocks.UpdateOrderCmd{
			ID:       "agg1",
			NewState: mocks.Order{ID: "1", Status: "Pending"},
		},
	)
	if err != nil {
		t.Fatalf(
			"Send: %v",
			err,
		)
	}

	if event.AggregateID != "agg1" {
		t.Fatalf(
			"AggregateID = %q, want agg1",
			event.AggregateID,
		)
	}
}

type failingFactory struct {
	err error
}

func (f failingFactory) build(
	asynxmd.DispatcherConfig[mocks.Order],
) (asynxmd.Dispatcher[mocks.Order], error) {
	return nil,
		f.err
}

func TestNew_ReturnsANilProcessorWhenTheFactoryFails(
	t *testing.T,
) {
	boom := errors.New("boom")
	failing := failingFactory{err: boom}
	p, err := processor.New[mocks.Order](
		newStore(),
		nil,
		models.WithDispatcherFactory(failing.build),
	)
	if !errors.Is(
		err,
		boom,
	) {
		t.Fatalf(
			"err = %v, want %v",
			err,
			boom,
		)
	}

	if p != nil {
		t.Fatalf(
			"processor = %v, want nil",
			p,
		)
	}
}

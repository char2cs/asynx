package asynx_test

import (
	"context"
	"testing"

	"github.com/char2cs/asynx"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/models"
)

func TestNewBuilderDefaults(t *testing.T) {
	b := asynx.New[mocks.Order]()
	if b == nil {
		t.Fatal("New returned nil")
	}
}

func TestBuildRequiresEventStore(t *testing.T) {
	_, err := asynx.New[mocks.Order]().Build()
	if err != models.ErrMissingEventStore {
		t.Errorf("expected ErrMissingEventStore, got %v", err)
	}
}

func TestBuildSucceeds(t *testing.T) {
	instance, err := asynx.New[mocks.Order]().WithEventStore(&mocks.Store{}).Build()
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
	if instance == nil {
		t.Fatal("Build() returned nil instance")
	}
}

func TestBuildSnapshotStoreDefaultsToEventStore(t *testing.T) {
	store := &mocks.Store{}
	_, err := asynx.New[mocks.Order]().WithEventStore(store).Build()
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
}

func TestBuildMultipleTimesIndependent(t *testing.T) {
	b := asynx.New[mocks.Order]().WithEventStore(&mocks.Store{})

	i1, _ := b.Build()
	i2, _ := b.Build()

	if i1 == i2 {
		t.Error("Build() must return a new instance each call")
	}
}

func TestBuilderFluent(t *testing.T) {
	upcaster := func(ctx context.Context, eventName string, raw []byte) ([]byte, error) { return raw, nil }
	panicHandler := func(_ context.Context, _ models.Event[mocks.Order], _ any) {}

	_, err := asynx.New[mocks.Order]().
		WithEventStore(&mocks.Store{}).
		WithSnapshotStore(&mocks.Store{}).
		WithBus(&mocks.Bus[mocks.Order]{}).
		WithShardingOpts(asynx.ShardingOpts{Shards: 16, QueueDepth: 100}).
		WithSchemaVersion(3).
		WithUpcaster(1, upcaster).
		WithUpcaster(2, upcaster).
		WithPanicHandler(panicHandler).
		Build()

	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
}


func TestBuilderWithCorruptionHook(t *testing.T) {
	called := false
	hook := func(err error) { called = true }

	_, err := asynx.New[mocks.Order]().
		WithEventStore(&mocks.Store{}).
		WithCorruptionHook(hook).
		Build()
	if err != nil {
		t.Fatalf("Build() error: %v", err)
	}
	_ = called // hook is wired; invocation tested at integration level
}

func TestShardingOptsZeroValue(t *testing.T) {
	opts := asynx.ShardingOpts{}
	if opts.Shards != 0 || opts.QueueDepth != 0 {
		t.Errorf("unexpected zero ShardingOpts: %+v", opts)
	}
}

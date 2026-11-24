package core

import (
	"context"
	"errors"
	"fmt"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher"
	"github.com/char2cs/asynx/internal/processor/internal/exec"
	"github.com/char2cs/asynx/internal/processor/internal/pool"
	"github.com/char2cs/asynx/internal/processor/internal/queue"
	"github.com/char2cs/asynx/internal/processor/models"
	asynxmd "github.com/char2cs/asynx/models"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

// Processor is the implementation behind the package-level Processor interface.
// It routes each command, by aggregate id, to one of a fixed number of shards;
// a shard worker grants the command its turn per aggregate (see
// models.CommandEnvelope) and the command runs through the executor's write
// pipeline under the aggregate's lock; the committed event is then handed to the
// dispatcher for delivery to subscribers and the bus.
//
// Life cycle: sends and forgets register in a wait group under lifeMu, so
// Shutdown can flip to "shutting down", wait for those in flight, drain the
// shards and close the dispatcher and bus, in that order. Nothing is stored
// from a caller's context. A Processor is safe for concurrent use; build it
// with New.
type Processor[T any] struct {
	es            eventstore.EventStore[T]
	pool          *pool.ShardPool[T]
	router        *queue.Router
	executor      *exec.CommandExecutor[T]
	dispatcher    asynxmd.Dispatcher[T]
	onPublishErr  asynxmd.PublishErrorHandler[T]
	bus           asynxmd.Bus[T]
	shuttingDown  atomic.Bool
	lifeMu        sync.Mutex
	shutdown      shutdownState
	sends         sync.WaitGroup
	forgets       sync.WaitGroup
	onSendPending func()
}

const (
	defaultRetryInitial = 100 * time.Millisecond
	defaultRetryMax     = 30 * time.Second
	defaultMaxAttempts  = 20
	defaultShards       = 8
	defaultWorkers      = 8
)

var errNilDispatcher = errors.New("asynx: the dispatcher factory returned no dispatcher")

// New builds a Processor. The dispatcher comes from the factory given by
// WithDispatcherFactory, or from the in-memory default when there is none and a
// bus is present. A factory error, or a factory that returns no dispatcher,
// fails New and nothing is left running.
func New[T any](
	es eventstore.EventStore[T],
	bus asynxmd.Bus[T],
	opts ...models.Opt[T],
) (*Processor[T], error) {
	cfg := configure(opts...)
	d, err := newDispatcher(
		cfg,
		bus,
	)
	if err != nil {
		return nil,
			err
	}

	router, err := queue.New(cfg.Shards)
	if err != nil {
		return nil,
			fmt.Errorf(
				"asynx: shard router: %w",
				err,
			)
	}

	return assemble(
			es,
			bus,
			d,
			router,
			cfg,
		),
		nil
}

func assemble[T any](
	es eventstore.EventStore[T],
	bus asynxmd.Bus[T],
	d asynxmd.Dispatcher[T],
	router *queue.Router,
	cfg *models.Config[T],
) *Processor[T] {
	executor := newExecutor(
		es,
		d,
		cfg,
	)
	return &Processor[T]{
		es: es,
		pool: pool.New[T](
			cfg.Shards,
			cfg.QueueDepth,
			cfg.WorkersPerShard,
		),
		router:       router,
		executor:     executor,
		dispatcher:   d,
		onPublishErr: cfg.PublishErrorHandler,
		bus:          bus,
	}
}

func newExecutor[T any](
	es eventstore.EventStore[T],
	d asynxmd.Dispatcher[T],
	cfg *models.Config[T],
) *exec.CommandExecutor[T] {
	executor := exec.New(
		es,
		d,
		exec.WithSyncProjections(cfg.SyncProjections),
		exec.WithDurableDelivery[T](cfg.DurableMatch),
		exec.WithSnapshotErrorHandler(cfg.SnapshotErrorHandler),
	)
	es.GuardAutoSnapshots(executor.WithLockIfFree)
	return executor
}

func newDispatcher[T any](
	cfg *models.Config[T],
	bus asynxmd.Bus[T],
) (asynxmd.Dispatcher[T], error) {
	factory := cfg.DispatcherFactory
	if factory == nil &&
		bus == nil {
		return nil,
			nil
	}

	if factory == nil {
		factory = defaultDispatcher[T]
	}

	cfg.Dispatcher.Bus = bus
	cfg.Dispatcher.OnPublishError = cfg.PublishErrorHandler
	d, err := factory(cfg.Dispatcher)
	if err != nil {
		return nil,
			fmt.Errorf(
				"asynx: dispatcher factory: %w",
				err,
			)
	}

	if isNil(d) {
		return nil,
			errNilDispatcher
	}

	return d,
		nil
}

func isNil(
	value any,
) bool {
	if value == nil {
		return true
	}

	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Pointer,
		reflect.Map,
		reflect.Func,
		reflect.Slice,
		reflect.Chan,
		reflect.Interface:
		return rv.IsNil()
	}

	return false
}

func configure[T any](
	opts ...models.Opt[T],
) *models.Config[T] {
	cfg := &models.Config[T]{
		Shards:          defaultShards,
		QueueDepth:      -1,
		WorkersPerShard: defaultWorkers,
		Dispatcher: asynxmd.DispatcherConfig[T]{
			RetryInitial: defaultRetryInitial,
			RetryMax:     defaultRetryMax,
			MaxAttempts:  defaultMaxAttempts,
		},
	}
	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.QueueDepth <= 0 {
		cfg.QueueDepth = cfg.WorkersPerShard
	}

	return cfg
}

func defaultDispatcher[T any](
	cfg asynxmd.DispatcherConfig[T],
) (asynxmd.Dispatcher[T], error) {
	return dispatcher.Dispatcher(cfg),
		nil
}

func discardPanic() {
	if r := recover(); r != nil {
		return
	}
}

func (p *Processor[T]) reportPublishError(
	ctx context.Context,
	event asynxmd.Event[T],
	err error,
) {
	if p.onPublishErr == nil {
		return
	}

	defer discardPanic()
	p.onPublishErr(
		ctx,
		event,
		err,
	)
}

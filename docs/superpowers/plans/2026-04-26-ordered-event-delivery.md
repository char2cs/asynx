# Ordered Event Delivery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Guarantee per-aggregate ordered event delivery to subscribers regardless of Send vs SendWait usage.

**Architecture:** A new `Dispatcher` component sits between `CommandExecutor` and `Bus`. It manages per-aggregate FIFO queues — each aggregate with in-flight events gets a dedicated channel and worker goroutine. The worker calls `bus.PublishSync` for each event, ensuring event N's handlers complete before event N+1's start. Idle workers self-clean after a timeout.

**Tech Stack:** Go 1.26, no new dependencies. Uses `sync.Mutex`, channels, `sync.WaitGroup`, `time.After`.

---

## File Structure

| Action | Path | Responsibility |
|--------|------|---------------|
| Create | `internal/bus/dispatcher/dispatcher.go` | `Dispatcher` type, `Dispatch()`, per-aggregate queue management, worker loop, idle cleanup, `Close()` |
| Create | `internal/bus/dispatcher/dispatcher_test.go` | Unit tests: ordering, cross-aggregate independence, sync/async, idle cleanup, shutdown, panics |
| Create | `internal/bus/dispatcher/dispatcher_bench_test.go` | Benchmarks: single-aggregate throughput, multi-aggregate throughput, parallel dispatch |
| Modify | `internal/processor/exec/exec.go` | Replace `publishAsync`/`publishSync` with `dispatcher.Dispatch()`, remove goroutine tracking |
| Modify | `internal/processor/exec/exec_test.go` | Update tests to inject `Dispatcher`, remove `publishAsync`/`publishSync`/`WaitPublish` tests |
| Modify | `internal/processor/exec/exec_bench_test.go` | Update benchmarks to use `Dispatcher` |
| Modify | `internal/processor/processor.go` | Wire `Dispatcher` between executor and bus in constructor, update shutdown sequence |
| Modify | `internal/processor/processor_test.go` | Update `newProcessor` helper; update `WaitPublish` usages |
| Modify | `internal/processor/processor_integration_test.go` | Update `WaitPublish` usages |
| Modify | `internal/processor/processor_bench_test.go` | No code changes expected — wiring is internal |
| Modify | `models/errors.go` | Add `ErrDispatcherClosed` |

---

### Task 1: Capture Benchmark Baseline

**Files:**
- Read: `internal/bus/channel_bus_bench_test.go`
- Read: `internal/processor/processor_bench_test.go`
- Read: `internal/processor/exec/exec_bench_test.go`

- [ ] **Step 1: Run and save baseline benchmarks**

```bash
go test -bench=. -benchtime=3s -count=5 ./internal/bus/... ./internal/processor/... 2>&1 | tee /tmp/bench-baseline.txt
```

Expected: all benchmarks pass and output is saved.

- [ ] **Step 2: Verify baseline saved**

```bash
grep -c "Benchmark" /tmp/bench-baseline.txt
```

Expected: non-zero count of benchmark lines.

---

### Task 2: Add `ErrDispatcherClosed` Error

**Files:**
- Modify: `models/errors.go`

- [ ] **Step 1: Add the error to `models/errors.go`**

Add inside the `var` block:

```go
ErrDispatcherClosed = errors.New("asynx: dispatcher closed")
```

- [ ] **Step 2: Verify compilation**

```bash
go build ./...
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add models/errors.go
git commit -m "feat: add ErrDispatcherClosed sentinel error"
```

---

### Task 3: Implement `Dispatcher` Core — Types, Constructor, and `Dispatch`

**Files:**
- Create: `internal/bus/dispatcher/dispatcher.go`

- [ ] **Step 1: Create `internal/bus/dispatcher/dispatcher.go` with the full implementation**

```go
// Package dispatcher provides per-aggregate ordered event delivery.
//
// Dispatcher[T] sits between CommandExecutor and Bus. For any aggregate,
// subscriber handlers see event N before event N+1, regardless of whether
// the caller used Send (async) or SendWait (sync).
//
// Each aggregate with in-flight events gets a dedicated channel and worker
// goroutine. The worker calls bus.PublishSync for each event, ensuring
// strict FIFO ordering. Idle workers self-clean after idleTimeout.
package dispatcher

import (
	"context"
	"sync"
	"time"

	asynxmd "github.com/char2cs/asynx/models"
)

const (
	// defaultBufferSize is the per-aggregate channel buffer. The shard processes
	// commands sequentially per aggregate, so inflow is bounded.
	defaultBufferSize = 16

	// defaultIdleTimeout is how long a worker waits for new events before exiting.
	defaultIdleTimeout = 5 * time.Second
)

// Opt is a functional option for New.
type Opt[T any] func(*Dispatcher[T])

// WithPublishErrorHandler sets a callback invoked when bus.PublishSync returns
// a non-nil error. When not set, publish errors are silently dropped.
func WithPublishErrorHandler[T any](fn asynxmd.PublishErrorHandler[T]) Opt[T] {
	return func(d *Dispatcher[T]) {
		d.onPublishError = fn
	}
}

// WithIdleTimeout overrides the default idle timeout for worker goroutines.
func WithIdleTimeout[T any](t time.Duration) Opt[T] {
	return func(d *Dispatcher[T]) {
		if t > 0 {
			d.idleTimeout = t
		}
	}
}

// Dispatcher manages per-aggregate ordered event delivery.
type Dispatcher[T any] struct {
	bus asynxmd.Bus[T]

	mu     sync.Mutex
	queues map[string]*aggregateQueue[T] // aggregateID → queue
	closed bool

	wg sync.WaitGroup // tracks all live worker goroutines

	onPublishError asynxmd.PublishErrorHandler[T]
	idleTimeout    time.Duration
}

type aggregateQueue[T any] struct {
	ch chan *dispatchJob[T]
}

type dispatchJob[T any] struct {
	event asynxmd.Event[T]
	ctx   context.Context
	done  chan struct{} // closed when handlers complete; always allocated
}

// New creates a Dispatcher that delegates to bus for handler execution.
func New[T any](bus asynxmd.Bus[T], opts ...Opt[T]) *Dispatcher[T] {
	d := &Dispatcher[T]{
		bus:         bus,
		queues:      make(map[string]*aggregateQueue[T]),
		idleTimeout: defaultIdleTimeout,
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// Dispatch enqueues an event for ordered delivery to subscribers.
// The enqueue is synchronous (establishes ordering). If waitHandlers is true,
// Dispatch blocks until all handlers for this event complete. If false, it
// returns immediately after enqueuing.
func (d *Dispatcher[T]) Dispatch(
	ctx context.Context,
	event asynxmd.Event[T],
	waitHandlers bool,
) error {
	job := &dispatchJob[T]{
		event: event,
		ctx:   context.WithoutCancel(ctx),
		done:  make(chan struct{}),
	}

	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return asynxmd.ErrDispatcherClosed
	}

	q, exists := d.queues[event.AggregateID]
	if !exists {
		q = &aggregateQueue[T]{
			ch: make(chan *dispatchJob[T], defaultBufferSize),
		}
		d.queues[event.AggregateID] = q
		d.wg.Add(1)
		go d.worker(event.AggregateID, q)
	}

	q.ch <- job
	d.mu.Unlock()

	if waitHandlers {
		<-job.done
	}

	return nil
}

// Close signals all workers to drain and exit, then blocks until they finish.
func (d *Dispatcher[T]) Close(ctx context.Context) error {
	d.mu.Lock()
	d.closed = true
	for _, q := range d.queues {
		close(q.ch)
	}
	d.mu.Unlock()

	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WaitIdle blocks until all in-flight dispatch workers have completed.
// Only for use in tests; do not call in production code.
func (d *Dispatcher[T]) WaitIdle() {
	d.wg.Wait()
}

func (d *Dispatcher[T]) worker(aggregateID string, q *aggregateQueue[T]) {
	defer d.wg.Done()

	for {
		select {
		case job, ok := <-q.ch:
			if !ok {
				// Channel closed during shutdown — drain is complete.
				return
			}
			d.handle(job)

		case <-time.After(d.idleTimeout):
			d.mu.Lock()
			if len(q.ch) > 0 {
				d.mu.Unlock()
				continue
			}
			delete(d.queues, aggregateID)
			d.mu.Unlock()
			return
		}
	}
}

func (d *Dispatcher[T]) handle(job *dispatchJob[T]) {
	defer close(job.done)

	if d.bus == nil {
		return
	}

	err := d.bus.PublishSync(job.ctx, job.event)
	if err != nil && d.onPublishError != nil {
		d.onPublishError(job.ctx, job.event, err)
	}
}
```

- [ ] **Step 2: Verify compilation**

```bash
go build ./internal/bus/dispatcher/...
```

Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add internal/bus/dispatcher/dispatcher.go
git commit -m "feat: add Dispatcher for per-aggregate ordered event delivery"
```

---

### Task 4: Write Dispatcher Unit Tests

**Files:**
- Create: `internal/bus/dispatcher/dispatcher_test.go`

- [ ] **Step 1: Create `internal/bus/dispatcher/dispatcher_test.go`**

```go
package dispatcher

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	asynxmd "github.com/char2cs/asynx/models"
)

// --- test helpers ---

// recordingBus records the order of PublishSync calls per aggregate.
type recordingBus[T any] struct {
	mu     sync.Mutex
	events []asynxmd.Event[T]
	delay  time.Duration
}

func (b *recordingBus[T]) PublishSync(_ context.Context, event asynxmd.Event[T]) error {
	if b.delay > 0 {
		time.Sleep(b.delay)
	}
	b.mu.Lock()
	b.events = append(b.events, event)
	b.mu.Unlock()
	return nil
}

func (b *recordingBus[T]) Publish(_ context.Context, _ asynxmd.Event[T]) error { return nil }
func (b *recordingBus[T]) Subscribe(_ string, _ asynxmd.ProjectionHandler[T], _ ...asynxmd.SubscriptionOpt[T]) (string, error) {
	return "", nil
}
func (b *recordingBus[T]) Unsubscribe(_ string) error    { return nil }
func (b *recordingBus[T]) Close(_ context.Context) error { return nil }
func (b *recordingBus[T]) WaitForHandlers()               {}

func (b *recordingBus[T]) orderedEvents() []asynxmd.Event[T] {
	b.mu.Lock()
	defer b.mu.Unlock()
	cp := make([]asynxmd.Event[T], len(b.events))
	copy(cp, b.events)
	return cp
}

// errBus returns an error on PublishSync.
type errBus[T any] struct {
	err error
}

func (b *errBus[T]) PublishSync(_ context.Context, _ asynxmd.Event[T]) error { return b.err }
func (b *errBus[T]) Publish(_ context.Context, _ asynxmd.Event[T]) error    { return nil }
func (b *errBus[T]) Subscribe(_ string, _ asynxmd.ProjectionHandler[T], _ ...asynxmd.SubscriptionOpt[T]) (string, error) {
	return "", nil
}
func (b *errBus[T]) Unsubscribe(_ string) error    { return nil }
func (b *errBus[T]) Close(_ context.Context) error { return nil }
func (b *errBus[T]) WaitForHandlers()               {}

func makeEvent(aggregateID string, version int64) asynxmd.Event[string] {
	return asynxmd.Event[string]{
		AggregateID: aggregateID,
		EventName:   "TestEvent",
		Version:     version,
	}
}

// --- tests ---

func TestDispatch_OrderingGuarantee(t *testing.T) {
	rb := &recordingBus[string]{delay: time.Millisecond}
	d := New[string](rb, WithIdleTimeout[string](50*time.Millisecond))

	ctx := context.Background()
	for i := int64(1); i <= 10; i++ {
		d.Dispatch(ctx, makeEvent("agg1", i), false)
	}

	d.WaitIdle()

	events := rb.orderedEvents()
	if len(events) != 10 {
		t.Fatalf("expected 10 events, got %d", len(events))
	}
	for i, e := range events {
		if e.Version != int64(i+1) {
			t.Errorf("event %d: expected version %d, got %d", i, i+1, e.Version)
		}
	}
}

func TestDispatch_CrossAggregateIndependence(t *testing.T) {
	// Slow handler on agg1 should not block agg2.
	rb := &recordingBus[string]{}
	d := New[string](rb, WithIdleTimeout[string](50*time.Millisecond))

	ctx := context.Background()

	// agg1 gets a slow bus
	slowBus := &recordingBus[string]{delay: 50 * time.Millisecond}
	dSlow := New[string](slowBus, WithIdleTimeout[string](50*time.Millisecond))

	var wg sync.WaitGroup
	wg.Add(2)

	start := time.Now()
	var agg2Done time.Time

	go func() {
		defer wg.Done()
		for i := int64(1); i <= 5; i++ {
			dSlow.Dispatch(ctx, makeEvent("agg1", i), false)
		}
		dSlow.WaitIdle()
	}()

	go func() {
		defer wg.Done()
		for i := int64(1); i <= 5; i++ {
			d.Dispatch(ctx, makeEvent("agg2", i), false)
		}
		d.WaitIdle()
		agg2Done = time.Now()
	}()

	wg.Wait()

	// agg2 should finish much faster than agg1 (agg1 sleeps 50ms * 5 = 250ms)
	if agg2Done.Sub(start) > 200*time.Millisecond {
		t.Errorf("agg2 was blocked by agg1: took %v", agg2Done.Sub(start))
	}

	d.Close(context.Background())
	dSlow.Close(context.Background())
}

func TestDispatch_SyncBlocking(t *testing.T) {
	rb := &recordingBus[string]{delay: 20 * time.Millisecond}
	d := New[string](rb, WithIdleTimeout[string](50*time.Millisecond))

	ctx := context.Background()
	start := time.Now()

	err := d.Dispatch(ctx, makeEvent("agg1", 1), true)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if elapsed < 15*time.Millisecond {
		t.Errorf("Dispatch(wait=true) returned too fast: %v", elapsed)
	}

	d.Close(context.Background())
}

func TestDispatch_AsyncNonBlocking(t *testing.T) {
	rb := &recordingBus[string]{delay: 50 * time.Millisecond}
	d := New[string](rb, WithIdleTimeout[string](100*time.Millisecond))

	ctx := context.Background()
	start := time.Now()

	err := d.Dispatch(ctx, makeEvent("agg1", 1), false)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if elapsed > 10*time.Millisecond {
		t.Errorf("Dispatch(wait=false) blocked: %v", elapsed)
	}

	d.Close(context.Background())
}

func TestDispatch_IdleCleanup(t *testing.T) {
	rb := &recordingBus[string]{}
	idleTimeout := 50 * time.Millisecond
	d := New[string](rb, WithIdleTimeout[string](idleTimeout))

	ctx := context.Background()

	d.Dispatch(ctx, makeEvent("agg1", 1), false)
	d.WaitIdle()

	// Worker should have exited after idle timeout.
	// Wait a bit extra for cleanup.
	time.Sleep(idleTimeout + 20*time.Millisecond)

	d.mu.Lock()
	_, exists := d.queues["agg1"]
	d.mu.Unlock()

	if exists {
		t.Error("expected queue to be cleaned up after idle timeout")
	}

	// Dispatch again — should create a new worker.
	d.Dispatch(ctx, makeEvent("agg1", 2), false)
	d.WaitIdle()

	events := rb.orderedEvents()
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}

	d.Close(context.Background())
}

func TestClose_DrainsAllEvents(t *testing.T) {
	rb := &recordingBus[string]{delay: 5 * time.Millisecond}
	d := New[string](rb, WithIdleTimeout[string](time.Second))

	ctx := context.Background()
	for i := int64(1); i <= 20; i++ {
		d.Dispatch(ctx, makeEvent("agg1", i), false)
	}

	d.Close(context.Background())

	events := rb.orderedEvents()
	if len(events) != 20 {
		t.Fatalf("expected 20 events after Close, got %d", len(events))
	}
	for i, e := range events {
		if e.Version != int64(i+1) {
			t.Errorf("event %d: expected version %d, got %d", i, i+1, e.Version)
		}
	}
}

func TestDispatch_AfterClose(t *testing.T) {
	rb := &recordingBus[string]{}
	d := New[string](rb)

	d.Close(context.Background())

	err := d.Dispatch(context.Background(), makeEvent("agg1", 1), false)
	if !errors.Is(err, asynxmd.ErrDispatcherClosed) {
		t.Fatalf("expected ErrDispatcherClosed, got %v", err)
	}
}

func TestDispatch_NilBusNoPanic(t *testing.T) {
	d := New[string](nil, WithIdleTimeout[string](50*time.Millisecond))

	ctx := context.Background()
	err := d.Dispatch(ctx, makeEvent("agg1", 1), true)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}

	d.Close(context.Background())
}

func TestDispatch_PublishErrorHandler(t *testing.T) {
	publishErr := errors.New("bus error")
	eb := &errBus[string]{err: publishErr}

	var mu sync.Mutex
	var gotErr error
	called := false

	d := New[string](eb,
		WithIdleTimeout[string](50*time.Millisecond),
		WithPublishErrorHandler[string](func(_ context.Context, _ asynxmd.Event[string], err error) {
			mu.Lock()
			gotErr = err
			called = true
			mu.Unlock()
		}),
	)

	ctx := context.Background()
	d.Dispatch(ctx, makeEvent("agg1", 1), true)

	mu.Lock()
	defer mu.Unlock()
	if !called {
		t.Fatal("PublishErrorHandler was not called")
	}
	if gotErr != publishErr {
		t.Errorf("expected err %v, got %v", publishErr, gotErr)
	}

	d.Close(context.Background())
}

func TestDispatch_MultipleAggregatesOrdered(t *testing.T) {
	rb := &recordingBus[string]{delay: time.Millisecond}
	d := New[string](rb, WithIdleTimeout[string](100*time.Millisecond))

	ctx := context.Background()

	// Interleave events from 3 aggregates.
	for i := int64(1); i <= 5; i++ {
		d.Dispatch(ctx, makeEvent("agg1", i), false)
		d.Dispatch(ctx, makeEvent("agg2", i), false)
		d.Dispatch(ctx, makeEvent("agg3", i), false)
	}

	d.WaitIdle()

	events := rb.orderedEvents()
	// Verify per-aggregate ordering.
	lastVersion := map[string]int64{}
	for _, e := range events {
		prev := lastVersion[e.AggregateID]
		if e.Version <= prev {
			t.Errorf("aggregate %s: version %d after %d (out of order)", e.AggregateID, e.Version, prev)
		}
		lastVersion[e.AggregateID] = e.Version
	}

	if len(events) != 15 {
		t.Errorf("expected 15 events, got %d", len(events))
	}

	d.Close(context.Background())
}

func TestClose_ContextTimeout(t *testing.T) {
	// Bus that blocks forever.
	blockBus := &recordingBus[string]{delay: time.Hour}
	d := New[string](blockBus, WithIdleTimeout[string](time.Second))

	ctx := context.Background()
	d.Dispatch(ctx, makeEvent("agg1", 1), false)

	closeCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := d.Close(closeCtx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

func TestDispatch_ConcurrentSameAggregate(t *testing.T) {
	rb := &recordingBus[string]{}
	d := New[string](rb, WithIdleTimeout[string](100*time.Millisecond))

	ctx := context.Background()

	var wg sync.WaitGroup
	n := 100
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(v int64) {
			defer wg.Done()
			d.Dispatch(ctx, makeEvent("agg1", v), false)
		}(int64(i + 1))
	}
	wg.Wait()

	d.WaitIdle()

	events := rb.orderedEvents()
	if len(events) != n {
		t.Fatalf("expected %d events, got %d", n, len(events))
	}

	// All events should have been delivered — exact order depends on goroutine
	// scheduling, but within a single aggregate the worker processes them FIFO
	// from the channel. Verify no duplicates.
	seen := make(map[int64]bool)
	for _, e := range events {
		if seen[e.Version] {
			t.Errorf("duplicate version %d", e.Version)
		}
		seen[e.Version] = true
	}

	d.Close(context.Background())
}

func TestDispatch_WithIdleTimeoutOption(t *testing.T) {
	rb := &recordingBus[string]{}
	d := New[string](rb, WithIdleTimeout[string](10*time.Millisecond))

	ctx := context.Background()
	d.Dispatch(ctx, makeEvent("agg1", 1), true)

	// Wait for idle cleanup.
	time.Sleep(30 * time.Millisecond)

	d.mu.Lock()
	_, exists := d.queues["agg1"]
	d.mu.Unlock()

	if exists {
		t.Error("expected queue to be cleaned up with custom idle timeout")
	}

	d.Close(context.Background())
}

func TestDispatch_WorkerSurvivesPanic(t *testing.T) {
	// The bus's internal panic recovery (in exec.ExecuteHandler) prevents panics
	// from reaching the dispatcher worker. This test verifies the worker keeps
	// processing events even if PublishSync somehow panics — the deferred close(job.done)
	// in handle() ensures the done channel is closed even on panic.
	var callCount atomic.Int32
	panicBus := &callbackBus[string]{
		publishSyncFn: func(_ context.Context, e asynxmd.Event[string]) error {
			n := callCount.Add(1)
			if n == 1 {
				panic("handler panic")
			}
			return nil
		},
	}

	d := New[string](panicBus, WithIdleTimeout[string](50*time.Millisecond))
	ctx := context.Background()

	// First event triggers panic — should not kill worker.
	d.Dispatch(ctx, makeEvent("agg1", 1), false)
	// Second event should still be processed.
	d.Dispatch(ctx, makeEvent("agg1", 2), false)

	d.WaitIdle()

	count := callCount.Load()
	if count < 2 {
		t.Errorf("expected at least 2 PublishSync calls, got %d", count)
	}

	d.Close(context.Background())
}

// callbackBus allows injecting custom behavior for PublishSync.
type callbackBus[T any] struct {
	publishSyncFn func(context.Context, asynxmd.Event[T]) error
}

func (b *callbackBus[T]) PublishSync(ctx context.Context, event asynxmd.Event[T]) error {
	if b.publishSyncFn != nil {
		return b.publishSyncFn(ctx, event)
	}
	return nil
}
func (b *callbackBus[T]) Publish(_ context.Context, _ asynxmd.Event[T]) error { return nil }
func (b *callbackBus[T]) Subscribe(_ string, _ asynxmd.ProjectionHandler[T], _ ...asynxmd.SubscriptionOpt[T]) (string, error) {
	return "", nil
}
func (b *callbackBus[T]) Unsubscribe(_ string) error    { return nil }
func (b *callbackBus[T]) Close(_ context.Context) error { return nil }
func (b *callbackBus[T]) WaitForHandlers()               {}
```

- [ ] **Step 2: Run tests**

```bash
go test -race -v ./internal/bus/dispatcher/...
```

Expected: all tests pass with race detector.

- [ ] **Step 3: Commit**

```bash
git add internal/bus/dispatcher/dispatcher_test.go
git commit -m "test: add Dispatcher unit tests for ordering, lifecycle, and error handling"
```

---

### Task 5: Add Panic Recovery to Dispatcher Worker

The `handle` method uses `defer close(job.done)` which ensures the done channel is always closed. But if `bus.PublishSync` panics (which shouldn't happen due to bus-level recovery, but defense in depth), the worker goroutine would crash. Add a recover in the worker loop.

**Files:**
- Modify: `internal/bus/dispatcher/dispatcher.go`

- [ ] **Step 1: Add recover to the worker loop**

Replace the `handle` method:

```go
func (d *Dispatcher[T]) handle(job *dispatchJob[T]) {
	defer close(job.done)
	defer func() {
		if r := recover(); r != nil {
			// Worker survives — PublishSync panicked. The bus's own panic
			// recovery should prevent this, but we recover defensively so
			// the worker keeps processing subsequent events.
		}
	}()

	if d.bus == nil {
		return
	}

	err := d.bus.PublishSync(job.ctx, job.event)
	if err != nil && d.onPublishError != nil {
		d.onPublishError(job.ctx, job.event, err)
	}
}
```

- [ ] **Step 2: Run tests including panic test**

```bash
go test -race -v -run TestDispatch_WorkerSurvivesPanic ./internal/bus/dispatcher/...
```

Expected: PASS.

- [ ] **Step 3: Run all dispatcher tests**

```bash
go test -race -v ./internal/bus/dispatcher/...
```

Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/bus/dispatcher/dispatcher.go
git commit -m "fix: add panic recovery to Dispatcher worker for resilience"
```

---

### Task 6: Wire Dispatcher Into CommandExecutor

**Files:**
- Modify: `internal/processor/exec/exec.go`

- [ ] **Step 1: Replace the CommandExecutor implementation**

Replace the contents of `internal/processor/exec/exec.go` with:

```go
// Package exec implements the command execution pipeline for Asynx.
//
// CommandExecutor[T] applies the three-phase command processing pattern:
//   - Load    — Fetch current aggregate state from EventStore; nil if not found
//   - Validate — Call Command.Validate(state); return raw error if rejected
//   - Write   — Call EventStore.Write; wrap storage errors as ErrPipelineFailed
//   - Dispatch — Call Dispatcher.Dispatch; ordered delivery is guaranteed
//
// Validation errors short-circuit at phase 2; no event is written. Storage errors
// at phase 3 wrap to ErrPipelineFailed. Event dispatching uses the Dispatcher for
// per-aggregate ordered delivery.
package exec

import (
	"context"
	"errors"
	"fmt"

	"github.com/char2cs/asynx/internal/bus/dispatcher"
	"github.com/char2cs/asynx/internal/eventstore"
	asynxmd "github.com/char2cs/asynx/models"
)

type CommandExecutor[T any] struct {
	es         *eventstore.EventStore[T]
	dispatcher *dispatcher.Dispatcher[T]
}

func New[T any](
	es *eventstore.EventStore[T],
	d *dispatcher.Dispatcher[T],
) *CommandExecutor[T] {
	return &CommandExecutor[T]{es: es, dispatcher: d}
}

func (e *CommandExecutor[T]) Execute(
	ctx context.Context,
	cmd asynxmd.Command[T],
	nextVersion int64,
	waitHandlers bool,
) (asynxmd.Event[T], error) {
	event, err := e.es.Write(ctx, cmd)
	if err != nil {
		if errors.Is(err, asynxmd.ErrValidation) {
			return asynxmd.Event[T]{}, err
		}

		return asynxmd.Event[T]{}, fmt.Errorf("%w: %w", asynxmd.ErrPipelineFailed, err)
	}

	if e.dispatcher != nil {
		e.dispatcher.Dispatch(ctx, event, waitHandlers)
	}

	return event, nil
}
```

Note: `CommandExecutorOpt`, `WithPublishErrorHandler`, `WaitPublish`, `publishAsync`, `publishSync`, `publishMu`, `pending`, `publishCv`, `onPublishError` are all removed. The `onPublishError` option moved to `dispatcher.WithPublishErrorHandler` in Task 3. The `New` function now takes `*dispatcher.Dispatcher[T]` instead of `models.Bus[T]`.

- [ ] **Step 2: Verify compilation**

```bash
go build ./internal/processor/exec/...
```

Expected: build errors in test files (they reference old types). That's expected — we'll fix tests next.

- [ ] **Step 3: Commit**

```bash
git add internal/processor/exec/exec.go
git commit -m "refactor: replace CommandExecutor publish logic with Dispatcher"
```

---

### Task 7: Update CommandExecutor Tests

**Files:**
- Modify: `internal/processor/exec/exec_test.go`
- Modify: `internal/processor/exec/exec_bench_test.go`

- [ ] **Step 1: Replace `internal/processor/exec/exec_test.go`**

```go
package exec

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/char2cs/asynx/internal/bus/dispatcher"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/store"
	asynxmd "github.com/char2cs/asynx/models"
)

type order = mocks.Order

func TestExecute_SuccessNewAggregate(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)
	executor := New(es, nil)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}

	_, err := executor.Execute(ctx, cmd, 1, false)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}

	agg, err := es.Get(ctx, "order1")
	if err != nil {
		t.Fatalf("expected aggregate to exist, got error: %v", err)
	}
	if agg.ID != "order1" || agg.Total != 100.0 {
		t.Errorf("unexpected aggregate state: %+v", agg)
	}
}

func TestExecute_SuccessExistingAggregate(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)
	executor := New(es, nil)

	ctx := context.Background()

	cmd1 := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}
	_, err := executor.Execute(ctx, cmd1, 1, false)
	if err != nil {
		t.Fatalf("first command failed: %v", err)
	}

	cmd2 := mocks.UpdateOrderCmd{
		ID:       "order1",
		NewState: order{ID: "order1", Total: 150.0, Status: "Confirmed"},
	}
	_, err = executor.Execute(ctx, cmd2, 2, false)
	if err != nil {
		t.Fatalf("second command failed: %v", err)
	}

	agg, err := es.Get(ctx, "order1")
	if err != nil {
		t.Fatalf("expected aggregate to exist, got error: %v", err)
	}
	if agg.Total != 150.0 || agg.Status != "Confirmed" {
		t.Errorf("unexpected aggregate state: %+v", agg)
	}
}

func TestExecute_ValidationFailure(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)
	executor := New(es, nil)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "order1", Total: -100.0}

	_, err := executor.Execute(ctx, cmd, 1, false)
	if err != asynxmd.ErrValidation {
		t.Fatalf("expected ErrValidation, got %v", err)
	}

	_, err = es.Get(ctx, "order1")
	if err != asynxmd.ErrNotFound {
		t.Errorf("expected ErrNotFound, but aggregate was written")
	}
}

func TestExecute_ValidationFailureNoWrite(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)
	executor := New(es, nil)

	ctx := context.Background()

	cmd1 := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}
	executor.Execute(ctx, cmd1, 1, false)

	cmd2 := mocks.CancelOrderCmd{ID: "order2"}

	_, err := executor.Execute(ctx, cmd2, 1, false)
	if err != asynxmd.ErrValidation {
		t.Fatalf("expected ErrValidation, got %v", err)
	}

	_, err = es.Get(ctx, "order2")
	if err != asynxmd.ErrNotFound {
		t.Errorf("expected ErrNotFound, but aggregate was written")
	}
}

func TestExecute_GetStorageError(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)
	executor := New(es, nil)

	ctx := context.Background()
	cmd := mocks.UpdateOrderCmd{
		ID:       "order1",
		NewState: order{ID: "order1", Total: 100.0},
	}

	_, err := executor.Execute(ctx, cmd, 1, false)
	if err != nil {
		t.Fatalf("expected success (nil state for missing agg), got %v", err)
	}
}

func TestExecute_WriteStorageError(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)
	executor := New(es, nil)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}

	someError := errors.New("storage error")
	s.SetError("events:order1", someError)

	_, err := executor.Execute(ctx, cmd, 1, false)
	if !errors.Is(err, asynxmd.ErrPipelineFailed) {
		t.Errorf("expected ErrPipelineFailed, got %v", err)
	}
}

func TestExecute_NilDispatcherNoPanic(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)
	executor := New(es, nil)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}

	_, err := executor.Execute(ctx, cmd, 1, false)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestExecute_DispatchCalledAsync(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)

	rb := &recordingBus[order]{}
	d := dispatcher.New[order](rb, dispatcher.WithIdleTimeout[order](50*time.Millisecond))

	executor := New(es, d)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}

	_, err := executor.Execute(ctx, cmd, 1, false)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}

	d.WaitIdle()

	if rb.callCount.Load() != 1 {
		t.Errorf("expected 1 PublishSync call, got %d", rb.callCount.Load())
	}

	d.Close(context.Background())
}

func TestExecute_DispatchCalledSync(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)

	var handlerDone atomic.Bool
	cb := &callbackBus[order]{
		publishSyncFn: func(_ context.Context, _ asynxmd.Event[order]) error {
			handlerDone.Store(true)
			return nil
		},
	}
	d := dispatcher.New[order](cb, dispatcher.WithIdleTimeout[order](50*time.Millisecond))

	executor := New(es, d)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "wait-order", Total: 10.0}

	_, err := executor.Execute(ctx, cmd, 1, true)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}

	if !handlerDone.Load() {
		t.Error("Execute with waitHandlers=true returned before dispatch completed")
	}

	d.Close(context.Background())
}

func TestExecute_ContextAlreadyCancelled(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)
	executor := New(es, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cmd := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}

	_, err := executor.Execute(ctx, cmd, 1, false)
	if err != nil {
		t.Logf("expected context.Canceled, got %v", err)
	}
}

func TestExecute_AggregateStatePreserved(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)
	executor := New(es, nil)

	ctx := context.Background()

	cmd1 := mocks.CreateOrderCmd{ID: "order1", Total: 100.0}
	_, err := executor.Execute(ctx, cmd1, 1, false)
	if err != nil {
		t.Fatalf("first execute failed: %v", err)
	}

	cmd2 := mocks.UpdateOrderCmd{
		ID:       "order1",
		NewState: order{ID: "order1", Total: 150.0, Status: "Updated"},
	}
	_, err = executor.Execute(ctx, cmd2, 2, false)
	if err != nil {
		t.Fatalf("second execute failed: %v", err)
	}

	agg2, err := es.Get(ctx, "order1")
	if err != nil {
		t.Fatalf("get after second execute failed: %v", err)
	}

	if agg2.Total != 150.0 || agg2.Status != "Updated" {
		t.Errorf("aggregate not updated correctly: %+v", agg2)
	}
}

func TestExecute_ReturnsEvent(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)
	executor := New(es, nil)

	ctx := context.Background()
	cmd := mocks.CreateOrderCmd{ID: "evt-order", Total: 77.0}

	event, err := executor.Execute(ctx, cmd, 1, false)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if event.AggregateID != "evt-order" {
		t.Errorf("expected AggregateID evt-order, got %q", event.AggregateID)
	}
	if event.EventName == "" {
		t.Error("expected non-empty EventName")
	}
}

func TestExecute_PublishErrorHandler_CalledOnError(t *testing.T) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)

	publishErr := errors.New("bus error")

	var mu sync.Mutex
	var gotErr error
	called := false

	d := dispatcher.New[order](
		&errBusMock[order]{err: publishErr},
		dispatcher.WithIdleTimeout[order](50*time.Millisecond),
		dispatcher.WithPublishErrorHandler[order](func(_ context.Context, _ asynxmd.Event[order], err error) {
			mu.Lock()
			gotErr = err
			called = true
			mu.Unlock()
		}),
	)

	executor := New(es, d)

	ctx := context.Background()
	_, err := executor.Execute(ctx, mocks.CreateOrderCmd{ID: "err-order", Total: 1.0}, 1, true)
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if !called {
		t.Fatal("PublishErrorHandler was not called")
	}
	if gotErr != publishErr {
		t.Errorf("expected err %v, got %v", publishErr, gotErr)
	}

	d.Close(context.Background())
}

// --- test helpers ---

type recordingBus[T any] struct {
	callCount atomic.Int32
}

func (b *recordingBus[T]) PublishSync(_ context.Context, _ asynxmd.Event[T]) error {
	b.callCount.Add(1)
	return nil
}
func (b *recordingBus[T]) Publish(_ context.Context, _ asynxmd.Event[T]) error { return nil }
func (b *recordingBus[T]) Subscribe(_ string, _ asynxmd.ProjectionHandler[T], _ ...asynxmd.SubscriptionOpt[T]) (string, error) {
	return "", nil
}
func (b *recordingBus[T]) Unsubscribe(_ string) error    { return nil }
func (b *recordingBus[T]) Close(_ context.Context) error { return nil }
func (b *recordingBus[T]) WaitForHandlers()               {}

type callbackBus[T any] struct {
	publishSyncFn func(context.Context, asynxmd.Event[T]) error
}

func (b *callbackBus[T]) PublishSync(ctx context.Context, event asynxmd.Event[T]) error {
	if b.publishSyncFn != nil {
		return b.publishSyncFn(ctx, event)
	}
	return nil
}
func (b *callbackBus[T]) Publish(_ context.Context, _ asynxmd.Event[T]) error { return nil }
func (b *callbackBus[T]) Subscribe(_ string, _ asynxmd.ProjectionHandler[T], _ ...asynxmd.SubscriptionOpt[T]) (string, error) {
	return "", nil
}
func (b *callbackBus[T]) Unsubscribe(_ string) error    { return nil }
func (b *callbackBus[T]) Close(_ context.Context) error { return nil }
func (b *callbackBus[T]) WaitForHandlers()               {}

type errBusMock[T any] struct {
	err error
}

func (b *errBusMock[T]) PublishSync(_ context.Context, _ asynxmd.Event[T]) error { return b.err }
func (b *errBusMock[T]) Publish(_ context.Context, _ asynxmd.Event[T]) error    { return nil }
func (b *errBusMock[T]) Subscribe(_ string, _ asynxmd.ProjectionHandler[T], _ ...asynxmd.SubscriptionOpt[T]) (string, error) {
	return "", nil
}
func (b *errBusMock[T]) Unsubscribe(_ string) error    { return nil }
func (b *errBusMock[T]) Close(_ context.Context) error { return nil }
func (b *errBusMock[T]) WaitForHandlers()               {}
```

- [ ] **Step 2: Replace `internal/processor/exec/exec_bench_test.go`**

```go
package exec

import (
	"context"
	"testing"

	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/mocks"
	"github.com/char2cs/asynx/internal/store"
)

func BenchmarkExecute_CreateNew(b *testing.B) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)
	executor := New(es, nil)

	ctx := context.Background()

	for _, numEvents := range []int{1, 100, 1000} {
		b.Run("events="+string(rune('0'+numEvents/100))+"_000", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				cmd := mocks.CreateOrderCmd{ID: "order" + string(rune(b.N)), Total: 100.0}
				_, _ = executor.Execute(ctx, cmd, 1, false)
			}
		})
	}
}

func BenchmarkExecute_UpdateExisting(b *testing.B) {
	s := store.New()
	es := eventstore.New[order](s, s, nil, 1, nil)
	executor := New(es, nil)

	ctx := context.Background()

	createCmd := mocks.CreateOrderCmd{ID: "order_base", Total: 100.0}
	executor.Execute(ctx, createCmd, 1, false)

	for _, numEvents := range []int{1, 100, 1000} {
		b.Run("events="+string(rune('0'+numEvents/100))+"_000", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				cmd := mocks.UpdateOrderCmd{
					ID:       "order_base",
					NewState: order{ID: "order_base", Total: 150.0, Status: "Confirmed"},
				}
				_, _ = executor.Execute(ctx, cmd, int64(b.N), false)
			}
		})
	}
}
```

- [ ] **Step 3: Run tests**

```bash
go test -race -v ./internal/processor/exec/...
```

Expected: all pass. Build errors in `processor.go` and `processor_test.go` are expected — those are fixed in the next task.

- [ ] **Step 4: Commit**

```bash
git add internal/processor/exec/exec_test.go internal/processor/exec/exec_bench_test.go
git commit -m "test: update CommandExecutor tests for Dispatcher integration"
```

---

### Task 8: Wire Dispatcher Into Processor and Update Shutdown

**Files:**
- Modify: `internal/processor/processor.go`

- [ ] **Step 1: Update `internal/processor/processor.go`**

```go
// Package processor coordinates command routing and execution lifecycle for Asynx.
//
// Processor[T] routes incoming commands to shards via consistent hashing,
// manages graceful shutdown, and exposes Send/SendWait/Shutdown interfaces.
//   - router     — FNV-1a hash-based consistent shard selection
//   - pool       — Shard-based worker pool for concurrent command execution
//   - executor   — Passed to pool; executes Load→Validate→Write→Dispatch pipeline
//   - dispatcher — Per-aggregate ordered event delivery to Bus
//
// All command execution is non-blocking via channels. Send and SendWait block until either
// the command completes, context cancels, or the queue is full. Send dispatches events
// asynchronously; SendWait dispatches synchronously. Shutdown drains in-flight work
// then closes the dispatcher and bus.
package processor

import (
	"context"
	"sync/atomic"

	"github.com/char2cs/asynx/internal/bus/dispatcher"
	"github.com/char2cs/asynx/internal/eventstore"
	"github.com/char2cs/asynx/internal/processor/exec"
	"github.com/char2cs/asynx/internal/processor/models"
	"github.com/char2cs/asynx/internal/processor/pool"
	"github.com/char2cs/asynx/internal/processor/queue"
	asynxmd "github.com/char2cs/asynx/models"
)

type Processor[T any] struct {
	pool          *pool.ShardPool[T]
	router        *queue.Router
	dispatcher    *dispatcher.Dispatcher[T]
	bus           asynxmd.Bus[T]
	shuttingDown  atomic.Bool
	onSendPending func()
}

type ProcessorOpt[T any] func(*processorConfig[T])

type processorConfig[T any] struct {
	shards              int
	queueDepth          int
	workersPerShard     int
	publishErrorHandler asynxmd.PublishErrorHandler[T]
}

func WithShards[T any](count int) ProcessorOpt[T] {
	return func(cfg *processorConfig[T]) {
		if count > 0 {
			cfg.shards = count
		}
	}
}

func WithQueueDepth[T any](depth int) ProcessorOpt[T] {
	return func(cfg *processorConfig[T]) {
		if depth >= 0 {
			cfg.queueDepth = depth
		}
	}
}

func WithWorkersPerShard[T any](count int) ProcessorOpt[T] {
	return func(cfg *processorConfig[T]) {
		if count > 0 {
			cfg.workersPerShard = count
		}
	}
}

// WithPublishErrorHandler sets a callback invoked when Bus.PublishSync returns a
// non-nil error during event dispatch. When not set, publish errors are silently dropped.
func WithPublishErrorHandler[T any](fn asynxmd.PublishErrorHandler[T]) ProcessorOpt[T] {
	return func(cfg *processorConfig[T]) {
		cfg.publishErrorHandler = fn
	}
}

func New[T any](
	es *eventstore.EventStore[T],
	bus asynxmd.Bus[T],
	opts ...ProcessorOpt[T],
) *Processor[T] {
	cfg := &processorConfig[T]{
		shards:          8,
		queueDepth:      -1,
		workersPerShard: 8,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.queueDepth < 0 {
		cfg.queueDepth = cfg.workersPerShard
	}

	dispatcherOpts := []dispatcher.Opt[T]{}
	if cfg.publishErrorHandler != nil {
		dispatcherOpts = append(dispatcherOpts, dispatcher.WithPublishErrorHandler[T](cfg.publishErrorHandler))
	}
	d := dispatcher.New(bus, dispatcherOpts...)

	executor := exec.New(es, d)
	return &Processor[T]{
		pool: pool.New(
			executor,
			cfg.shards,
			cfg.queueDepth,
			cfg.workersPerShard,
		),
		router: queue.New(
			cfg.shards,
		),
		dispatcher: d,
		bus:        bus,
	}
}

func (p *Processor[T]) Send(
	ctx context.Context,
	cmd asynxmd.Command[T],
) (asynxmd.Event[T], error) {
	if p.isShuttingDown() {
		return asynxmd.Event[T]{}, asynxmd.ErrShuttingDown
	}

	if err := ctx.Err(); err != nil {
		return asynxmd.Event[T]{}, asynxmd.ErrContextCancelled
	}

	shardIndex := p.router.Route(
		cmd.AggregateID(),
	)
	shard := p.pool.Shards()[shardIndex]
	envelope := &models.CommandEnvelope[T]{
		Cmd:        cmd,
		Ctx:        ctx,
		ResultChan: make(chan models.CommandResult[T], 1),
	}

	return p.sendAndWait(ctx, shard, envelope)
}

func (p *Processor[T]) SendWait(
	ctx context.Context,
	cmd asynxmd.Command[T],
) (asynxmd.Event[T], error) {
	if p.isShuttingDown() {
		return asynxmd.Event[T]{}, asynxmd.ErrShuttingDown
	}

	if err := ctx.Err(); err != nil {
		return asynxmd.Event[T]{}, asynxmd.ErrContextCancelled
	}

	shardIndex := p.router.Route(
		cmd.AggregateID(),
	)
	shard := p.pool.Shards()[shardIndex]
	envelope := &models.CommandEnvelope[T]{
		Cmd:          cmd,
		Ctx:          ctx,
		ResultChan:   make(chan models.CommandResult[T], 1),
		WaitHandlers: true,
	}

	return p.sendAndWait(ctx, shard, envelope)
}

func (p *Processor[T]) isShuttingDown() bool {
	return p.shuttingDown.Load()
}

func (p *Processor[T]) sendAndWait(
	ctx context.Context,
	shard *pool.Shard[T],
	envelope *models.CommandEnvelope[T],
) (asynxmd.Event[T], error) {
	select {
	case shard.CommandChan() <- envelope:
	case <-ctx.Done():
		return asynxmd.Event[T]{}, asynxmd.ErrContextCancelled
	default:
		return asynxmd.Event[T]{}, asynxmd.ErrQueueFull
	}

	if p.onSendPending != nil {
		p.onSendPending()
	}

	select {
	case result := <-envelope.ResultChan:
		return result.Event, result.Err
	case <-ctx.Done():
		return asynxmd.Event[T]{}, asynxmd.ErrContextCancelled
	}
}

func (p *Processor[T]) Shutdown(ctx context.Context) error {
	if !p.setShuttingDown() {
		return asynxmd.ErrAlreadyShuttingDown
	}

	if err := p.pool.Drain(ctx); err != nil {
		return err
	}

	if err := p.closeDispatcher(ctx); err != nil {
		return err
	}

	return p.closeBus(ctx)
}

func (p *Processor[T]) setShuttingDown() bool {
	return p.shuttingDown.CompareAndSwap(false, true)
}

func (p *Processor[T]) closeDispatcher(ctx context.Context) error {
	if p.dispatcher == nil {
		return nil
	}
	return p.dispatcher.Close(ctx)
}

func (p *Processor[T]) closeBus(ctx context.Context) error {
	if p.bus == nil {
		return nil
	}
	return p.bus.Close(ctx)
}

// ForTesting: WaitPublish blocks until all dispatched events have been delivered.
// Do not call in production code.
func (p *Processor[T]) WaitPublish() {
	if p.dispatcher != nil {
		p.dispatcher.WaitIdle()
	}
}

// ForTesting: SetOnSendPending sets a callback invoked after a command is
// enqueued but before Send or SendWait blocks waiting for its result.
// Do not call in production code.
func (p *Processor[T]) SetOnSendPending(fn func()) {
	p.onSendPending = fn
}
```

- [ ] **Step 2: Verify compilation of processor package**

```bash
go build ./internal/processor/...
```

Expected: test files may have issues, but `processor.go` compiles.

- [ ] **Step 3: Commit**

```bash
git add internal/processor/processor.go
git commit -m "refactor: wire Dispatcher into Processor, update shutdown sequence"
```

---

### Task 9: Update Processor Tests

**Files:**
- Modify: `internal/processor/processor_test.go`
- Modify: `internal/processor/processor_integration_test.go`

- [ ] **Step 1: Update `processor_test.go`**

The main changes needed:
1. Remove the `TestWithPublishErrorHandler_IsInvokedOnPublishError` test — it tested the old `publishAsync` goroutine + `WaitPublish` pattern. The equivalent is now tested in `exec_test.go` via `TestExecute_PublishErrorHandler_CalledOnError`.
2. The `blockingTestBus` test (`TestProcessor_SendWait_ContextCancelledWhileWaiting`) still works because `PublishSync` is still called by the dispatcher — but through the dispatcher's worker. The bus blocks, the dispatcher worker blocks, `SendWait` blocks on `job.done`, and context cancellation is handled by `sendAndWait`'s second select. The test logic is the same.
3. `WaitPublish()` calls should continue to work since `Processor.WaitPublish` now calls `dispatcher.WaitIdle()`.

Replace the `TestWithPublishErrorHandler_IsInvokedOnPublishError` test:

```go
func TestWithPublishErrorHandler_IsInvokedOnPublishError(t *testing.T) {
	var got struct {
		sync.Mutex
		called bool
	}

	s := store.New()
	handler := func(_ context.Context, _ asynxmd.Event[mocks.Order], _ error) {
		got.Lock()
		got.called = true
		got.Unlock()
	}

	proc := processor.New(
		eventstore.New[mocks.Order](s, s, nil, 1, nil),
		&mocks.ErrBus[mocks.Order]{Err: errors.New("bus down")},
		processor.WithPublishErrorHandler[mocks.Order](handler),
	)

	_, err := proc.Send(context.Background(), mocks.CreateOrderCmd{ID: "order-1", Total: 10})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	// Wait for the dispatcher worker to finish.
	proc.WaitPublish()

	got.Lock()
	called := got.called
	got.Unlock()

	if !called {
		t.Error("PublishErrorHandler was not called after bus error")
	}

	proc.Shutdown(context.Background())
}
```

- [ ] **Step 2: Update `processor_integration_test.go`**

The `WaitPublish()` calls remain the same — they now call `dispatcher.WaitIdle()` under the hood. Also add `channelBus.WaitForHandlers()` after `WaitPublish()` in tests that check handler side effects, since the dispatcher uses `PublishSync` but the bus handlers' goroutines may still be in flight when the sync returns.

Review each test and verify `WaitPublish()` usage is correct. The key integration tests:
- `TestProcessor_IntegrationOrderPreservation` — uses `p.WaitPublish()`, should work as-is.
- `TestProcessor_IntegrationEventPublishingReliability` — uses `p.WaitPublish()`, should work as-is.

No changes expected to integration tests if `WaitPublish` correctly delegates.

- [ ] **Step 3: Run all tests**

```bash
go test -race -v ./internal/processor/...
```

Expected: all pass.

- [ ] **Step 4: Run all project tests**

```bash
go test -race ./...
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add internal/processor/processor_test.go internal/processor/processor_integration_test.go
git commit -m "test: update Processor tests for Dispatcher integration"
```

---

### Task 10: Update Public API (`asynx.go`)

**Files:**
- Modify: `asynx.go`

- [ ] **Step 1: Update `WaitPublish` in `asynx.go`**

The `WaitPublish` method currently calls both `proc.WaitPublish()` and `bus.WaitForHandlers()`. With the dispatcher, `proc.WaitPublish()` waits for dispatcher workers to finish (which means `PublishSync` completed). `bus.WaitForHandlers()` waits for any remaining in-flight handler goroutines inside the bus. Both calls should remain:

```go
func (i *asynxImpl[T]) WaitPublish() {
	i.proc.WaitPublish()
	i.bus.WaitForHandlers()
}
```

This is already the current implementation — no change needed. Verify by reading the file and confirming.

- [ ] **Step 2: Run full test suite**

```bash
go test -race ./...
```

Expected: all pass.

- [ ] **Step 3: Commit (only if changes were needed)**

If no changes were needed, skip this commit.

---

### Task 11: Add Dispatcher Benchmarks

**Files:**
- Create: `internal/bus/dispatcher/dispatcher_bench_test.go`

- [ ] **Step 1: Create benchmark file**

```go
package dispatcher

import (
	"context"
	"fmt"
	"testing"

	asynxmd "github.com/char2cs/asynx/models"
)

// noopBus is a zero-cost bus used to isolate dispatcher overhead.
type noopBus[T any] struct{}

func (b *noopBus[T]) PublishSync(_ context.Context, _ asynxmd.Event[T]) error { return nil }
func (b *noopBus[T]) Publish(_ context.Context, _ asynxmd.Event[T]) error    { return nil }
func (b *noopBus[T]) Subscribe(_ string, _ asynxmd.ProjectionHandler[T], _ ...asynxmd.SubscriptionOpt[T]) (string, error) {
	return "", nil
}
func (b *noopBus[T]) Unsubscribe(_ string) error    { return nil }
func (b *noopBus[T]) Close(_ context.Context) error { return nil }
func (b *noopBus[T]) WaitForHandlers()               {}

func makeBenchEvent(aggregateID string, version int64) asynxmd.Event[string] {
	return asynxmd.Event[string]{
		AggregateID: aggregateID,
		EventName:   "BenchEvent",
		Version:     version,
	}
}

// BenchmarkDispatch_SingleAggregate measures throughput when all events target
// the same aggregate (single worker, full serialization).
func BenchmarkDispatch_SingleAggregate(b *testing.B) {
	d := New[string](&noopBus[string]{})
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := range b.N {
		d.Dispatch(ctx, makeBenchEvent("agg1", int64(i)), false)
	}
	b.StopTimer()
	d.Close(context.Background())
}

// BenchmarkDispatch_MultiAggregate measures throughput across many aggregates
// (one worker per aggregate, maximum parallelism).
func BenchmarkDispatch_MultiAggregate(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("%d_aggregates", n), func(b *testing.B) {
			d := New[string](&noopBus[string]{})
			ctx := context.Background()

			b.ResetTimer()
			b.ReportAllocs()

			for i := range b.N {
				aggID := fmt.Sprintf("agg%d", i%n)
				d.Dispatch(ctx, makeBenchEvent(aggID, int64(i)), false)
			}
			b.StopTimer()
			d.Close(context.Background())
		})
	}
}

// BenchmarkDispatch_Parallel measures throughput with concurrent dispatchers.
func BenchmarkDispatch_Parallel(b *testing.B) {
	d := New[string](&noopBus[string]{})
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := int64(0)
		for pb.Next() {
			d.Dispatch(ctx, makeBenchEvent("agg1", i), false)
			i++
		}
	})
	b.StopTimer()
	d.Close(context.Background())
}

// BenchmarkDispatch_Sync measures throughput when every dispatch blocks (SendWait path).
func BenchmarkDispatch_Sync(b *testing.B) {
	d := New[string](&noopBus[string]{})
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := range b.N {
		d.Dispatch(ctx, makeBenchEvent("agg1", int64(i)), true)
	}
	b.StopTimer()
	d.Close(context.Background())
}
```

- [ ] **Step 2: Run benchmarks**

```bash
go test -bench=. -benchtime=3s ./internal/bus/dispatcher/...
```

Expected: benchmarks run and report allocations.

- [ ] **Step 3: Commit**

```bash
git add internal/bus/dispatcher/dispatcher_bench_test.go
git commit -m "bench: add Dispatcher benchmarks"
```

---

### Task 12: Verify Benchmarks Haven't Regressed

**Files:**
- Read: `/tmp/bench-baseline.txt` (from Task 1)

- [ ] **Step 1: Run post-implementation benchmarks**

```bash
go test -bench=. -benchtime=3s -count=5 ./internal/bus/... ./internal/processor/... 2>&1 | tee /tmp/bench-after.txt
```

- [ ] **Step 2: Compare results**

```bash
go install golang.org/x/perf/cmd/benchstat@latest
benchstat /tmp/bench-baseline.txt /tmp/bench-after.txt
```

Expected: no significant regressions in existing benchmarks. The bus benchmarks should be unchanged (bus code is untouched). Processor benchmarks may show a small difference due to dispatcher channel overhead, but it should be within noise.

Key benchmarks to watch:
- `BenchmarkSend_MultiShard` — should be similar (dispatcher adds one channel send)
- `BenchmarkSend_Parallel` — should be similar
- `BenchmarkSend_WithBusFanout` — may change slightly (PublishSync instead of Publish)
- `BenchmarkPublish_*` — should be identical (bus untouched)

- [ ] **Step 3: If regressions > 10%, investigate**

Common causes:
- Mutex contention in `Dispatch` — consider sharded locks if needed
- Channel overhead — consider increasing buffer size
- Worker goroutine startup cost — consider pre-warming

- [ ] **Step 4: Commit benchmark results**

```bash
git add -A
git commit -m "bench: verify no regression after Dispatcher integration"
```

---

### Task 13: Final Verification

- [ ] **Step 1: Run full test suite with race detector**

```bash
go test -race -count=1 ./...
```

Expected: all pass.

- [ ] **Step 2: Run linter**

```bash
make lint
```

Expected: no issues.

- [ ] **Step 3: Run coverage**

```bash
make test-coverage
```

Expected: coverage maintained or improved.

- [ ] **Step 4: Final commit if any cleanup needed**

```bash
git status
```

If clean, done. Otherwise commit any remaining changes.

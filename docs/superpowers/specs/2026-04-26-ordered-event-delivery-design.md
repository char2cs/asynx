# Per-Aggregate Ordered Event Delivery

## Problem

When commands for the same aggregate use different publish modes (`Send` vs `SendWait`), subscriber handlers can observe events out of order.

The shard processes commands sequentially, so events are produced in order. But the current dispatch mechanism breaks ordering at two levels:

1. **CommandExecutor level:** `publishAsync` (used by `Send`) spawns a goroutine that eventually calls `bus.Publish`. The goroutine for event N might reach the bus after event N+1's inline `publishSync` has already completed.

2. **ChannelBus level:** `Publish` spawns one independent goroutine per subscriber with no ordering. Even if two `Publish` calls happen in order, their handler goroutines race.

This breaks the event sourcing invariant that projections observe events in aggregate order. It forces application code to choose `Send` vs `SendWait` based on broadcast ordering concerns rather than the actual question: "does the caller need to block?"

## Solution

A new `Dispatcher` component that sits between `CommandExecutor` and `Bus`, providing per-aggregate FIFO event delivery.

### Architecture

```
Processor --> Executor --> Dispatcher --> Bus (ChannelBus)
                            |
                   per-aggregate queues
                   (one worker goroutine each)
```

The `Dispatcher` depends on the `models.Bus[T]` interface, not on `ChannelBus` directly. Any bus implementation works.

### Core Types

Located in `internal/bus/dispatcher/dispatcher.go`:

```go
type Dispatcher[T any] struct {
    bus       models.Bus[T]

    mu        sync.Mutex
    queues    map[string]*aggregateQueue[T]  // aggregateID -> queue
    closed    bool

    wg        sync.WaitGroup  // tracks all live worker goroutines

    onPublishError  models.PublishErrorHandler[T]  // moved from CommandExecutor
}

type aggregateQueue[T any] struct {
    ch   chan *dispatchJob[T]
}

type dispatchJob[T any] struct {
    event  models.Event[T]
    ctx    context.Context
    done   chan struct{}  // closed when handlers complete; always allocated
}
```

### Dispatch Flow

`Dispatch(ctx, event, waitHandlers bool)`:

1. Lock `mu`.
2. If `closed`, unlock and return error.
3. Look up queue for `event.AggregateID`.
   - If no queue exists: create channel (buffer size 16), start worker goroutine, store in map.
4. Build `dispatchJob` with a fresh `done` channel.
5. Enqueue job into the aggregate's channel.
6. Unlock `mu`.
7. If `waitHandlers == true`: block on `<-job.done`.
8. If `waitHandlers == false`: return immediately.

The enqueue (step 5) happens synchronously inside `Execute()`, before the shard worker moves to the next command. This establishes ordering at the source.

### Worker Loop

One goroutine per active aggregate. Two modes depending on dispatcher state:

**Steady state (channel open):**

```go
func (d *Dispatcher[T]) worker(aggregateID string, q *aggregateQueue[T]) {
    defer d.wg.Done()

    for {
        select {
        case job, ok := <-q.ch:
            if !ok {
                return  // channel closed during shutdown
            }
            d.handle(job)

        case <-time.After(idleTimeout):
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
```

**`handle(job)`:**

Calls `bus.PublishSync(job.ctx, job.event)`, then closes `job.done`. Handlers for event N complete before the worker picks up event N+1.

### Idle Cleanup

- Workers use a select with an idle timeout of 5 seconds (configurable via functional option).
- When the timeout fires and the channel is empty, the worker removes itself from the map and exits.
- On the next event for that aggregate, `Dispatch` creates a fresh queue and worker.
- The gap is safe: the old worker fully drained before exiting, so no events are lost.

**Resource cost:** one goroutine (~4KB stack) per aggregate with in-flight events. Idle aggregates cost nothing.

### Shutdown

The shutdown sequence becomes:

```
Processor.Shutdown()
  |
  +- 1. Pool.Drain(ctx)          // wait for shard workers to finish
  |                                // no more Execute() calls after this
  |
  +- 2. Dispatcher.Close(ctx)    // drain all queues, wait for workers
  |
  +- 3. Bus.Close(ctx)           // wait for remaining in-flight handlers
```

**`Dispatcher.Close(ctx)` contract:**

1. Set `closed = true`.
2. Close all queue channels (workers drain remaining jobs and exit via `for range`).
3. Wait for `d.wg.Wait()`, respecting context deadline.

Once `closed = true`, `Dispatch` returns an error. Ordering is preserved during shutdown: each worker finishes its current event's handlers before moving to the next.

### CommandExecutor Changes

**Before:**

```go
type CommandExecutor[T any] struct {
    es             *eventstore.EventStore[T]
    bus            models.Bus[T]
    publishMu      sync.Mutex
    pending        int
    publishCv      *sync.Cond
    onPublishError models.PublishErrorHandler[T]
}

func (e *CommandExecutor[T]) Execute(...) {
    ...
    if waitHandlers {
        e.publishSync(ctx, event)
    } else {
        e.publishAsync(ctx, event)
    }
}
```

**After:**

```go
type CommandExecutor[T any] struct {
    es         *eventstore.EventStore[T]
    dispatcher *dispatcher.Dispatcher[T]
}

func (e *CommandExecutor[T]) Execute(...) {
    ...
    e.dispatcher.Dispatch(ctx, event, waitHandlers)
}
```

**Removed from `CommandExecutor`:**
- `publishAsync` method (goroutine spawning)
- `publishSync` method
- `publishMu`, `pending`, `publishCv` (condition variable machinery)
- `WaitPublish()` (dispatcher owns in-flight tracking)
- `onPublishError` (moves to dispatcher)
- `bus` field (no longer referenced directly)

**Net effect:** `exec.go` gets shorter. The goroutine management and synchronization code moves out; what remains is the clean load -> validate -> write -> dispatch pipeline.

### Processor Wiring

```go
func New[T any](es *eventstore.EventStore[T], bus models.Bus[T], opts...) *Processor[T] {
    ...
    d := dispatcher.New(bus, dispatcherOpts...)
    executor := exec.New(es, d, execOpts...)
    ...
}
```

The processor creates the dispatcher and passes it to the executor. The bus reference stays on the processor for `Subscribe`, `Unsubscribe`, and `Close`.

### Buffer Size

The per-aggregate channel buffer is a small constant (16). The shard processes commands sequentially per aggregate, so the inflow rate is bounded. The buffer absorbs the gap between enqueue and the worker picking up the job.

### Panic Handling

If a handler panics during `bus.PublishSync`, the bus's existing panic recovery handles it (via `PanicHandler`). The dispatcher's worker is unaffected because the panic is recovered inside `exec.ExecuteHandler` before `PublishSync` returns. The worker proceeds to the next event.

## Testing Strategy

### New Tests (`internal/bus/dispatcher/dispatcher_test.go`)

1. **Ordering guarantee** -- dispatch events N, N+1, N+2 for the same aggregate (async). Record handler invocation order. Assert strict FIFO.
2. **Cross-aggregate independence** -- two aggregates dispatching concurrently. A slow handler on aggregate X must not block aggregate Y.
3. **Sync blocking** -- `Dispatch(ctx, event, true)` must not return until handlers complete.
4. **Async non-blocking** -- `Dispatch(ctx, event, false)` must return before handlers complete.
5. **Idle cleanup** -- dispatch an event, wait longer than idle timeout, dispatch another. Assert a new worker was created. Assert ordering still holds.
6. **Shutdown drains** -- enqueue several events, call `Close`. Assert all were delivered in order before `Close` returns.
7. **Dispatch after close** -- call `Dispatch` after `Close`. Assert error returned.
8. **Panic in handler** -- handler panics. Assert next event for that aggregate still gets delivered.

### Modified Tests

- `internal/processor/exec/exec_test.go` -- inject `Dispatcher` instead of bus. Remove tests for `publishAsync`/`publishSync`/`WaitPublish` (that logic is gone).

### Unchanged Tests

- `internal/bus/channel_bus_test.go` -- completely untouched.
- `internal/processor/processor_integration_test.go` -- pass as-is since public API is unchanged. These validate end-to-end wiring.
- `asynx_test.go` -- untouched.

## Scope

### New files
- `internal/bus/dispatcher/dispatcher.go` (~150-200 lines)
- `internal/bus/dispatcher/dispatcher_test.go` (~200-300 lines)

### Modified files
- `internal/processor/exec/exec.go` -- replace publish methods with dispatcher call (net reduction)
- `internal/processor/exec/exec_test.go` -- update to inject dispatcher
- `internal/processor/processor.go` -- wire dispatcher in constructor

### Unchanged
- `internal/bus/channel_bus.go`
- `models/bus.go`
- `internal/processor/pool/` (shard, pool)
- `internal/processor/queue/` (router)
- `asynx.go` (public API)

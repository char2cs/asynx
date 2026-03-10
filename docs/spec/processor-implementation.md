# Processor Implementation Specification

## Overview

This specification details the **internal architecture and mechanics** of the processor module. It explains how components interact, how concurrency is managed, and how the command execution pipeline actually works at the implementation level.

The processor module orchestrates command execution through a sharded worker pool with channels-based synchronization. No mutexes. No shared state between shards. Everything communicates via channels.

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                         Processor[T]                         │
│  (router, pool, executor, shutdownCoordinator, eventstore)  │
└────────────────────┬────────────────────────────────────────┘
                     │
         ┌───────────┴───────────┐
         │                       │
    ┌────▼─────────┐     ┌──────▼──────────┐
    │   Router     │     │  ShardPool[T]   │
    │              │     │                 │
    │ Hash-based   │     │  [Shard0]       │
    │ routing      │     │  [Shard1]       │
    │              │     │  ...            │
    │ O(1) lookup  │     │  [ShardN]       │
    └──────────────┘     └──────┬──────────┘
                                │
                    ┌───────────┴───────────┐
                    │                       │
              ┌─────▼──────────┐    ┌──────▼──────────┐
              │     Shard 0    │    │     Shard K    │
              │                │    │                │
              │  Queue (chan)  │    │  Queue (chan)  │
              │    ↓           │    │    ↓           │
              │  Worker(0)     │    │  Worker(K)     │
              │                │    │                │
              │  Executor      │    │  Executor      │
              │  Eventstore    │    │  Eventstore    │
              │  Bus           │    │  Bus           │
              └────────────────┘    └────────────────┘
```

---

## Core Internal Types

### 1. `CommandEnvelope[T]` — Command + Result Channel

```go
type CommandEnvelope[T any] struct {
    cmd        Command[T]
    ctx        context.Context
    resultChan chan error          // size 1, buffered
}
```

**Purpose:** Wraps a command with its context and a result channel so the caller can block waiting for the result.

**Lifecycle:**
```
1. Caller creates CommandEnvelope with buffered result channel
2. Caller sends to shard queue: shardQueue <- envelope
3. Caller blocks: err := <-envelope.resultChan
4. Worker receives envelope from queue
5. Worker executes command
6. Worker sends result: envelope.resultChan <- err
7. Caller unblocks
```

**Buffering:** `resultChan` is **buffered (size 1)** so the worker can send the result without blocking, even if the caller's context is cancelled and it's no longer listening.

**Cancellation Handling:**
```go
// If context is cancelled while waiting in queue:
select {
case <-ctx.Done():
    // Attempt to remove envelope from queue (if not yet dequeued)
    // Send cancellation signal to worker if already being processed
    return ErrContextCancelled
case err := <-resultChan:
    return err
}
```

---

### 2. `Shard[T]` — Independent Processing Unit

```go
type Shard[T any] struct {
    id              int
    commandChan     chan *CommandEnvelope[T]  // buffered, size = QueueDepth
    workerCtx       context.Context
    workerCancel    context.CancelFunc
    stopChan        chan struct{}             // signals worker to stop
    doneWg          *sync.WaitGroup           // tracks worker completion

    // Internal state owned by worker goroutine (read-only from outside)
    // Worker keeps version counter per aggregate locally
    versionMap      map[string]int64          // aggregateID → next version
}
```

**Properties:**
- **Independent**: Each shard is completely isolated from others
- **FIFO Processing**: Worker processes commands from `commandChan` in order
- **No Shared State**: Only the shard's worker goroutine accesses `versionMap`
- **Graceful Shutdown**: `stopChan` signals worker to stop accepting and drain queue

**Invariants:**
- Commands for the same aggregate always route to the same shard (hash routing)
- Commands in a shard are processed sequentially, in order
- Version numbers are monotonic per aggregate: 1, 2, 3, ... (no gaps)

---

### 3. `ShardPool[T]` — Manages All Shards

```go
type ShardPool[T any] struct {
    shards         []*Shard[T]
    numShards      int
    queueDepth     int

    shutdownOnce   sync.Once
    shutdownDone   chan struct{}  // closed when all shards drained
}
```

**Responsibilities:**
- Create N shards at startup
- Maintain them throughout lifetime
- Drain them gracefully on shutdown
- Track completion with WaitGroup

**WaitGroup Lifecycle:**

```go
// At startup, in ShardPool.Start():
for i := 0; i < numShards; i++ {
    p.doneWg.Add(1)  // Increment for each worker goroutine
    go p.shards[i].run()  // Worker is responsible for calling Done()
}

// In shard worker run() function:
defer p.doneWg.Done()  // Decrement when exiting
for {
    select {
    case <-stopChan:
        drainQueue()
        return  // Implicitly calls Done() via defer
    // ... process commands ...
    }
}

// During shutdown, in Processor.Shutdown():
for _, shard := range pool.shards {
    close(shard.stopChan)  // Signal all workers to stop
}

// Wait for all workers to exit:
done := make(chan struct{})
go func() {
    pool.doneWg.Wait()  // Blocks until all Done() calls complete
    close(done)
}()

select {
case <-done:
    return nil  // All workers exited
case <-ctx.Done():
    return ctx.Err()  // Shutdown timeout exceeded
}
```

---

### 4. `Router` — Hash-Based Routing

```go
type Router struct {
    numShards int
}

func (r *Router) Route(aggregateID string) int {
    h := fnv.New64a()
    h.Write([]byte(aggregateID))
    return int(h.Sum64() % uint64(r.numShards))
}
```

**Algorithm:**
- FNV-1a hash (or similar deterministic hash)
- `hash(aggregateID) % numShards` → shard index (0 to N-1)
- **Deterministic**: Same aggregateID always → same shard
- **Fast**: O(1) lookup, no locks

**Distribution:**
- Ideally uniform across shards
- Handles "hot" aggregates by concentrating them on a shard (intentional, serial)

---

### 5. `CommandExecutor[T]` — Pipeline Orchestrator

```go
type CommandExecutor[T any] struct {
    eventstore EventStore[T]
    bus        Bus[T]
}

func (e *CommandExecutor[T]) Execute(
    ctx context.Context,
    cmd Command[T],
    nextVersion int64,
) error {
    // Synchronous phase (caller blocks)

    // Step 1: Load current state
    currentState, err := e.eventstore.Get(ctx, cmd.AggregateID())
    if err != nil && err != ErrNotFound {
        return ErrPipelineFailed
    }

    // Handle nil for first event
    var state *T
    if err == ErrNotFound {
        state = nil  // First event, no prior state
    } else {
        state = &currentState
    }

    // Step 2: Validate command
    if err := cmd.Validate(state); err != nil {
        return ErrValidation  // Command invalid, no event created
    }

    // Step 3: Emit new state
    newState := cmd.EmitEvent(state)

    // Step 4: Write to eventstore (SAVE POINT)
    event, err := e.eventstore.Write(
        ctx,
        cmd.AggregateID(),
        cmd.EventName(),
        *state,           // oldState (or zero value if first)
        newState,
        nextVersion,
        cmd.ShouldSnapshot(),
    )
    if err != nil {
        return ErrPipelineFailed  // Could be version conflict, storage error, etc.
    }

    // CALLER UNBLOCKS HERE
    // Event is durable, safe to return

    // Asynchronous phase (async goroutine in worker)
    go func() {
        // Use context values from caller (trace ID, request ID, etc.)
        // but detached from deadline/cancellation (so async publish completes independently)
        ctxWithValues := context.WithoutCancel(ctx)

        if err := e.bus.Publish(ctxWithValues, event); err != nil {
            // Event is already durable, failure to publish is not fatal
            // Log error, increment metric, but don't crash
            log.Printf("failed to publish event %s: %v\n", event.EventName, err)
        }
    }()

    return nil
}
```

**Key Points:**
- Validation and write are **synchronous** (caller blocks)
- Publish is **asynchronous** (caller already free)
- Version is passed in (shard worker incremented it)
- Event is durable before returning
- Publish errors are logged, not propagated

---

## Worker Pool (Shard Workers)

Each shard has a **pool of worker goroutines** (e.g., 4-8 workers) that process commands from a shared job queue sequentially *per aggregate*. This design prevents goroutine explosion while maintaining serial ordering.

### Architecture

```
CommandChan (buffered, size=QueueDepth)
        ↓
   Job Queue (channel)
   (one per shard)
        ↓
   [Worker 1] [Worker 2] [Worker 3] [Worker 4]
        ↓           ↓           ↓           ↓
   All pull from same queue, execute independently
   Each updates versionMap for aggregates it processes
```

### Implementation

```go
const WorkersPerShard = 4  // Tunable, e.g., 2-8

type Shard[T any] struct {
    id              int
    commandChan     chan *CommandEnvelope[T]  // Buffered, size = QueueDepth
    jobQueue        chan *commandJob[T]        // Unbuffered, internal
    workerCtx       context.Context
    workerCancel    context.CancelFunc
    stopChan        chan struct{}
    doneWg          *sync.WaitGroup

    versionMap      map[string]int64
}

type commandJob[T any] struct {
    envelope *CommandEnvelope[T]
    nextVersion int64
}

func (s *Shard[T]) Start(executor *CommandExecutor[T]) {
    // Start the dispatcher goroutine
    // This forwards commands from commandChan to jobQueue
    go s.dispatchCommands()

    // Start N worker goroutines
    for i := 0; i < WorkersPerShard; i++ {
        s.doneWg.Add(1)
        go s.workerLoop(executor, i)
    }
}

// Dispatcher: Moves commands from public queue to job queue
// Maintains versionMap updates
func (s *Shard[T]) dispatchCommands() {
    for {
        select {
        case <-s.stopChan:
            // Stop accepting new commands, close job queue
            close(s.jobQueue)
            return

        case envelope := <-s.commandChan:
            if envelope == nil {
                close(s.jobQueue)
                return
            }

            // Increment version in dispatcher (single point)
            aggregateID := envelope.cmd.AggregateID()
            nextVersion := s.versionMap[aggregateID] + 1
            s.versionMap[aggregateID] = nextVersion

            // Send job to workers
            s.jobQueue <- &commandJob[T]{
                envelope:    envelope,
                nextVersion: nextVersion,
            }
        }
    }
}

// Worker: Processes jobs from the job queue
func (s *Shard[T]) workerLoop(executor *CommandExecutor[T], workerID int) {
    defer s.doneWg.Done()

    for job := range s.jobQueue {  // Blocks until job available or channel closed
        s.executeJob(executor, job, workerID)
    }
    // When jobQueue closes, range exits and worker exits
}

func (s *Shard[T]) executeJob(
    executor *CommandExecutor[T],
    job *commandJob[T],
    workerID int,
) {
    envelope := job.envelope
    nextVersion := job.nextVersion

    // Execute in a goroutine so we can monitor context independently
    resultChan := make(chan error, 1)
    go func() {
        resultChan <- executor.Execute(envelope.ctx, envelope.cmd, nextVersion)
    }()

    // Wait for either execution to complete or context to be cancelled
    select {
    case err := <-resultChan:
        // Execution completed
        if err != nil && err == asynx.ErrValidation {
            // Validation failed: decrease versionMap to avoid gap
            aggregateID := envelope.cmd.AggregateID()
            s.versionMap[aggregateID]--
        }
        envelope.resultChan <- err

    case <-envelope.ctx.Done():
        // Context was cancelled, return error immediately
        // Note: executor goroutine may still be running
        envelope.resultChan <- ErrContextCancelled
    }
}
```

### Benefits of Worker Pool vs. Per-Command Goroutine

**Per-Command Goroutine (Old Design):**
```
QueueDepth=10000, Shards=8
Maximum goroutines: 8 × 10000 = 80,000 goroutines
Memory: ~2KB per goroutine = 160 MB just for goroutines
GC pressure: Significant
```

**Worker Pool (New Design):**
```
QueueDepth=10000, Shards=8, WorkersPerShard=4
Maximum goroutines: 8 × 4 = 32 goroutines
Memory: ~2KB per goroutine = 64 KB for workers
GC pressure: Minimal
```

### Sequential Ordering Per Aggregate (Still Guaranteed)

Even with 4 workers per shard, commands for the same aggregate are processed **sequentially** because:

1. Dispatcher increments version atomically (single goroutine)
2. Each worker processes jobs with assigned versions
3. Version conflicts are impossible per aggregate (same shard always routes to same version sequence)

Example:
```
Dispatcher assigns:
  Command 1 for order_123: version 1 → Job to Worker 1
  Command 2 for order_456: version 1 → Job to Worker 2
  Command 3 for order_123: version 2 → Job to Worker 3
  Command 4 for order_123: version 3 → Job to Worker 1

Result:
  order_123 processes with versions 1, 2, 3 (sequential)
  order_456 processes with version 1 (different aggregate)
  Different aggregates run on different workers in parallel
  Same aggregate may run on different workers, but always in version order
```

func (s *Shard[T]) processCommand(
    executor *CommandExecutor[T],
    envelope *CommandEnvelope[T],
) {
    cmd := envelope.cmd
    aggregateID := cmd.AggregateID()

    // Increment version for this aggregate
    nextVersion := s.versionMap[aggregateID] + 1
    s.versionMap[aggregateID] = nextVersion

    // Execute in a goroutine so we can monitor context independently
    resultChan := make(chan error, 1)
    go func() {
        resultChan <- executor.Execute(envelope.ctx, cmd, nextVersion)
    }()

    // Wait for either execution to complete or context to be cancelled
    select {
    case err := <-resultChan:
        // Execution completed (event may or may not be durable depending on when cancellation happened)
        envelope.resultChan <- err

    case <-envelope.ctx.Done():
        // Context was cancelled
        // Return error immediately to caller
        // Note: resultChan goroutine may still be running (we don't wait for it)
        envelope.resultChan <- ErrContextCancelled
    }
}

// Note: With the dispatcher pattern, graceful shutdown is automatic.
// When stopChan closes, the dispatcher closes jobQueue.
// Workers then exit as they finish processing jobs (jobQueue is drained).
```

**Execution Order:**
```
Worker loops indefinitely:
  1. Wait for command or stop signal
  2. If stop signal: drain queue and exit
  3. If command: process it (validate, emit, write)
  4. Send result to caller
  5. Go back to step 1
```

**Version Management:**
- Shard worker maintains `versionMap[aggregateID]`
- Before processing command: increment version
- Pass version to executor
- Executor includes version in eventstore.Write()
- If write fails (version conflict in multi-node): caller must retry Send() from scratch (which reloads state, so new version will be higher)

**Cancellation Handling:**
- Monitor `envelope.ctx.Done()` in parallel
- If context cancelled before processing starts: return ErrContextCancelled
- If context cancelled during processing: executor respects context deadline (eventstore.Get and eventstore.Write both take context)

---

## Command Flow — Complete Example

```
Caller: instance.Send(ctx, cmd)

  ↓

Processor.Send():
  1. Check if shutting down
     if shutting down → return ErrShuttingDown

  2. Route command
     shardIndex := router.Route(cmd.AggregateID())
     shard := pool.shards[shardIndex]

  3. Create CommandEnvelope
     envelope := &CommandEnvelope{
         cmd:        cmd,
         ctx:        ctx,
         resultChan: make(chan error, 1),
     }

  4. Send to shard queue (non-blocking with three outcomes)
     select {
     case shard.commandChan <- envelope:
         // ✅ Successfully queued
         // Envelope is now in the buffered channel waiting for worker
         // Proceed to blocking on resultChan

     case <-ctx.Done():
         // ⏱️ Context cancelled before we could queue
         // Queue operation was interrupted
         return ctx.Err()  // Usually ErrContextCancelled

     default:
         // ❌ Channel is full (reached QueueDepth limit)
         // No 'case' matched, so we hit the default
         // Return immediately without queuing
         return ErrQueueFull
     }

  5. Block waiting for result
     select {
     case err := <-envelope.resultChan:
         return err  // Command executed, caller unblocks
     case <-ctx.Done():
         // Context cancelled while waiting
         return ErrContextCancelled
     }

  ↓

Worker Goroutine (on Shard):
  1. Receive envelope from commandChan
  2. Extract command and context
  3. Increment version: nextVersion = versionMap[aggregateID] + 1
  4. Call executor.Execute(ctx, cmd, nextVersion)

     a. eventstore.Get(ctx, aggregateID)
        → Load current state (warm or cold path)
     b. cmd.Validate(currentState)
        → Check if command is valid
        If error: return ErrValidation immediately
     c. cmd.EmitEvent(currentState)
        → Generate new state
     d. eventstore.Write(...)
        → Diff, serialize, append to stream
        If error (e.g., version conflict): return ErrPipelineFailed
     e. Return nil
        SAVE POINT: Event is durable

  5. Send result to caller
     envelope.resultChan <- err
     (buffered, doesn't block even if caller cancelled)

  6. Spawn async goroutine (async publish with context values)
     go func() {
         // Use caller's context values (trace ID, etc.)
         // but detached from deadline/cancellation
         ctxWithValues := context.WithoutCancel(envelope.ctx)
         executor.bus.Publish(ctxWithValues, event)
     }()

  7. Continue loop, wait for next command

  ↓

Caller unblocks:
  err := <-envelope.resultChan

  if err == nil:
      // Event is durable, projections will be notified eventually
  else if err == ErrValidation:
      // Command was invalid, try different command
  else if err == ErrPipelineFailed:
      // Write failed (likely version conflict)
      // Must retry Send() from scratch
      instance.Send(ctx, cmd)  // Reloads state, revalidates
```

---

## Shutdown Sequence — Complete Implementation

### Phase 1: Stop Intake

```go
func (p *Processor[T]) Shutdown(ctx context.Context) error {
    p.shutdownMutex.Lock()
    if p.shuttingDown {
        p.shutdownMutex.Unlock()
        return ErrAlreadyShuttingDown
    }
    p.shuttingDown = true
    p.shutdownMutex.Unlock()

    // From now on, Send() returns ErrShuttingDown immediately
    // In-flight Send() calls continue (not interrupted)
}
```

**Timing:** Immediate, synchronous.

**Effect:**
```go
func (p *Processor[T]) Send(ctx context.Context, cmd Command[T]) error {
    p.shutdownMutex.RLock()
    shuttingDown := p.shuttingDown
    p.shutdownMutex.RUnlock()

    if shuttingDown {
        return ErrShuttingDown  // NEW: return immediately
    }

    // ... rest of Send logic
}
```

---

### Phase 2: Drain Shards

```go
func (p *Processor[T]) drainShards(ctx context.Context) error {
    // Signal all shard workers to stop accepting new work
    for _, shard := range p.pool.shards {
        close(shard.stopChan)  // Non-blocking close
    }

    // Wait for all shards to finish draining
    // Use context deadline for timeout
    done := make(chan struct{})
    go func() {
        p.pool.doneWg.Wait()  // Wait for all worker goroutines to exit
        close(done)
    }()

    select {
    case <-done:
        // All shards drained
        return nil
    case <-ctx.Done():
        // Timeout exceeded
        return ctx.Err()
    }
}
```

**Behavior with Dispatcher + Worker Pool:**
```
When stopChan is closed:
  Dispatcher detects stopChan close signal
  ↓
  Dispatcher stops forwarding new commands from commandChan
  ↓
  Dispatcher closes jobQueue (signals all workers to stop accepting jobs)
  ↓
  All N workers finish their current job (if any) and pick up next from jobQueue
  ↓
  When jobQueue is closed, workers' `for job := range jobQueue` exits
  ↓
  Each worker calls doneWg.Done() via defer
  ↓
  When all workers exit, WaitGroup.Wait() returns
```

**Timing:** Could take seconds if workers are mid-execution. Graceful shutdown does NOT forcefully kill workers.

**Remaining Commands:** Commands still in `commandChan` are NOT processed. Only jobs already forwarded to `jobQueue` are guaranteed to execute. This is acceptable because Phase 1 has already stopped accepting new Send() calls.

**Process:**
```
Commander 1: Send() → queue command, block
CommandEnvelope 1: In queue, waiting

Phase 1: Stop intake
  New Send() calls return ErrShuttingDown
  But CommandEnvelope 1 is still in queue

Phase 2: Drain shards
  Signal worker to stop accepting new work
  Worker processes CommandEnvelope 1
  Worker increments version, calls executor.Execute()
  Executor: Get → Validate → Emit → Write (sync, blocks)
  Executor: Publish (async, worker doesn't wait)
  CommandEnvelope 1 result sent to Commander 1
  Commander 1 unblocks with nil (event durable)
  Worker drains remaining queue (none)
  Worker exits

  Meanwhile, async goroutine from executor is still running:
    Trying to publish event to bus
    (This runs concurrently with Phase 3)
```

---

### Phase 3: Drain Bus

```go
func (p *Processor[T]) drainBus(ctx context.Context) error {
    // Wait for bus to finish all in-flight publishes
    return p.bus.Close(ctx)
}
```

**Implementation (in bus):**
```go
func (b *ChannelBus[T]) Close(ctx context.Context) error {
    // Mark bus as closed
    b.mu.Lock()
    b.closed = true
    b.mu.Unlock()

    // Wait for all in-flight publishes to complete
    // Each publish spawns handler execution
    // Wait for all handlers to finish (or recover from panic)

    done := make(chan struct{})
    go func() {
        b.inFlightWg.Wait()  // Wait for all handlers
        close(done)
    }()

    select {
    case <-done:
        return nil
    case <-ctx.Done():
        return ctx.Err()
    }
}
```

**Process:**
```
Bus has in-flight publishes:
  publish(event1) → handler1 running
  publish(event2) → handler2 running

When Close() is called:
  Mark bus as closed (new publishes rejected)
  Wait for handler1 to finish
  Wait for handler2 to finish
  (If either panics, recover and continue)
  Return
```

---

## Concurrency Model — Channels Only

### Why No Mutexes?

1. **Each shard is independent** — No shared state between workers
2. **Worker owns its state** — Only that worker reads/writes `versionMap`
3. **Communication via channels** — All coordination happens through channels
4. **WaitGroup for synchronization** — Tracks when goroutines exit

### Communication Patterns

**Pattern 1: Caller ↔ Worker**
```
Caller sends command:
  shard.commandChan <- envelope

Worker receives:
  envelope := <-shard.commandChan

Worker sends result:
  envelope.resultChan <- err

Caller receives:
  err := <-envelope.resultChan
```

**Pattern 2: Shutdown Coordination**
```
Shutdown coordinator signals:
  close(shard.stopChan)

Worker detects:
  case <-s.stopChan:
      s.drainQueue(executor)
      return

Coordinator waits:
  p.pool.doneWg.Wait()
```

**Pattern 3: Context Cancellation**
```
Caller's context cancelled:
  <-ctx.Done()  // Returns immediately

Select in processor:
  case <-ctx.Done():
      return ErrContextCancelled

Or executor respects context:
  eventstore.Get(ctx, ...)  // Returns context error
  eventstore.Write(ctx, ...) // Returns context error
```

---

## Version Management — No Centralized Counter

### Design Choice: Shard-Local Versions

**Traditional Approach (Rejected):**
```go
// Global version counter (requires lock)
var globalVersion int64
var mu sync.Mutex

func nextVersion() int64 {
    mu.Lock()
    defer mu.Unlock()
    globalVersion++
    return globalVersion
}
```

**Problem:** Contention under high concurrency.

**Asynx Approach (Shard-Local):**
```go
// Each shard maintains versions per aggregate (no lock needed)
shard.versionMap[aggregateID]++
```

**How It Works:**
```
Shard 0 processes orders with ID "order_1":
  Command 1: versionMap["order_1"] = 0 → 1
  Command 2: versionMap["order_1"] = 1 → 2
  Command 3: versionMap["order_1"] = 2 → 3

Shard 1 processes orders with ID "order_2":
  Command 1: versionMap["order_2"] = 0 → 1
  Command 2: versionMap["order_2"] = 1 → 2

(Different aggregates, different shards, no contention)
```

**Multi-Node Handling:**
```
Node A (Shard 0): order_1 at version 3
Node B (Shard 0): order_1 at version 3

Both try to write version 4 simultaneously
  Node A writes (aggregateID="order_1", version=4) → succeeds
  Node B writes (aggregateID="order_1", version=4) → CONFLICT

Node B's write fails (uniqueness violation)
Store returns error to eventstore.Write()
Executor returns ErrPipelineFailed
Caller must retry Send() from scratch:
  Reloads state (now sees Node A's version 4)
  Revalidates
  Re-emits (against version 4 state)
  Tries again (now uses version 5)
```

### Version Map Initialization

Each shard initializes its `versionMap` as an empty map at startup:

```go
shard.versionMap = make(map[string]int64)
```

**First command for an aggregate:**
```go
// Aggregate "order_123" never processed before
nextVersion := s.versionMap["order_123"] + 1  // 0 + 1 = 1
s.versionMap["order_123"] = 1
// Event written at version 1
```

**Subsequent commands:**
```go
// Aggregate "order_123" already at version 3
nextVersion := s.versionMap["order_123"] + 1  // 3 + 1 = 4
s.versionMap["order_123"] = 4
// Event written at version 4
```

### Version Map Cleanup Strategy

The version map is a performance optimization — it caches the next version for frequently-accessed aggregates to avoid re-reading from storage. However, it can grow unbounded if new aggregates are continuously created.

**Two strategies are available:**

**Strategy A: Periodic Flush (Simple)**
```go
// Every 1000 commands, flush the entire map
if s.commandCount % 1000 == 0 {
    s.versionMap = make(map[string]int64)
}
```
**Tradeoff:** Next access to any aggregate incurs a slight penalty (re-read from storage), but memory is bounded.

**Strategy B: LRU Cache (Advanced)**
```go
// Use an LRU cache with max 10,000 entries
versionMap := lru.New[string, int64](10000)
```
**Tradeoff:** More complex, but entries are evicted intelligently by access frequency.

**Recommended:** Start with Strategy A (periodic flush). The storage read is fast and transparent to callers.

### Fairness and Starvation Prevention

All shards run at equal priority on independent worker goroutines. There is no starvation:

```
Shard 0: Processing 1000 commands/sec for "hot_aggregate"
Shard 1: Processing 10 commands/sec for "cold_aggregate"

Both run concurrently on separate goroutines.
Throughput for "cold_aggregate" is unaffected by load on "hot_aggregate".
```

**Exception:** If a single machine has fewer CPU cores than shards, OS scheduler determines allocation. In practice, goroutines are lightweight and will all get CPU time proportionally.

### Processor Restart Recovery

When a processor instance restarts, the `versionMap` in each shard is reset to empty. This is **safe** because it's just a performance cache.

**Scenario: Restart After Crash**
```
Before crash:
  Shard 0: versionMap["order_123"] = 5
  EventStore: order_123 has versions 1-5

Crash → Restart

After restart:
  Shard 0: versionMap["order_123"] = 0 (reset)
  EventStore: order_123 still has versions 1-5

Next command for order_123:
  nextVersion := versionMap["order_123"] + 1  // 0 + 1 = 1
  Append to eventstore: (order_123, version=1)
  ERROR: Version 1 already exists!

  Processor returns ErrPipelineFailed to caller
  Caller retries Send() from scratch

  Eventstore.Get() reloads order_123 state
  State is at version 5
  Caller revalidates and re-emits with fresh state
  Next command tries version 6
  SUCCESS: Append to eventstore: (order_123, version=6)
```

**Recovery Cost:** One extra failed write + retry per aggregate after restart. This is transparent to the caller (they see ErrPipelineFailed, which is a normal error requiring retry).

**Why This Is Safe:**
- Version map is only a cache to avoid store lookups
- Store is the source of truth (eventstore.Get() always reflects current state)
- Version conflicts are detected and trigger retries
- No data loss or corruption

**Optimization:** For aggregates with very long histories and high restart frequency, use `Preload()` at startup to pay the cold path cost offline and pre-populate the version map.

---

## Error Handling — Which Phase Failed?

### Synchronous Phase Errors

These happen before the save point and are returned synchronously:

**ErrValidation** — from `cmd.Validate()`
```go
if err := cmd.Validate(state); err != nil {
    // Command is invalid given current state
    // Event was NOT created
    // Caller can try different command
    return ErrValidation
}
```

**ErrPipelineFailed** — from `eventstore.Write()`
```go
if err != nil {
    // Write failed (version conflict, storage error, context cancelled)
    // Event was NOT created (or partially created and lost)
    // Caller must retry Send() from scratch
    return ErrPipelineFailed
}
```

**ErrQueueFull** — from channel send
```go
select {
case shard.commandChan <- envelope:
    // Queued successfully
default:
    // Channel full (buffered depth reached)
    // Command was NOT queued
    // Caller should back off and retry
    return ErrQueueFull
}
```

**ErrShuttingDown** — from shutdown flag
```go
if p.shuttingDown {
    // Processor is shutting down
    // Command was NOT queued
    // No recovery possible
    return ErrShuttingDown
}
```

**ErrContextCancelled** — from context
```go
select {
case err := <-envelope.resultChan:
    return err
case <-ctx.Done():
    // Context was cancelled
    // Command may or may not have been processed
    // If processed, event IS durable
    // If not processed, command was lost
    return ErrContextCancelled
}
```

---

### Asynchronous Phase Errors

These happen AFTER the save point, error is NOT propagated:

**bus.Publish() error**
```go
go func() {
    // This runs asynchronously
    // Use context values from original caller (trace ID, etc.)
    // but detached from deadline/cancellation
    ctxWithValues := context.WithoutCancel(envelope.ctx)

    if err := e.bus.Publish(ctxWithValues, event); err != nil {
        // Event is already durable
        // Publish failed (dispatch to subscribers failed)
        // Log error, metric, but don't crash
        log.Printf("failed to publish event: %v\n", err)
        // Caller never sees this error
    }
}()
```

**Projection handler panic**
```go
// Handler panics during execution
func(e asynx.Event[Order]) {
    panic("something went wrong")
}()

// Bus recovers:
defer func() {
    if r := recover(); r != nil {
        // Log, metric, call panic handler
        // Continue to next subscription
    }
}()
```

---

## Concurrent Send() Calls to Same Aggregate

All commands for the same aggregate route to the same shard and are processed sequentially:

```
Caller 1: Send(ctx1, OrderShipCmd for order_123)
Caller 2: Send(ctx2, OrderCancelCmd for order_123)

Router:
  hash("order_123") % numShards = Shard 5

Shard 5 queue:
  [OrderShipCmd, OrderCancelCmd]

Worker:
  Process OrderShipCmd:
    version = 1
    Validate (order status = "paid") ✓
    Emit (status = "shipped")
    Write (aggregateID="order_123", version=1, ...)
    Result → Caller 1

  Process OrderCancelCmd:
    version = 2
    Validate (order status = "shipped") ✗ (cannot cancel shipped order)
    Return ErrValidation
    Result → Caller 2
```

**Guarantee:** Commands for the same aggregate are **always processed in order**, so state transitions are predictable.

---

## Concurrent Send() Calls to Different Aggregates

Commands for different aggregates are processed **in parallel** on different shards:

```
Caller 1: Send(ctx1, OrderShipCmd for order_123)
Caller 2: Send(ctx2, OrderShipCmd for order_456)

Router:
  hash("order_123") % 8 = Shard 0
  hash("order_456") % 8 = Shard 5

Shard 0 worker:         Shard 5 worker:
  Process order_123     Process order_456
  Get state (fast)      Get state (slow cold path)
  Validate (fast)       Validate (slow)
  Emit (fast)           Emit (fast)
  Write (medium)        Write (medium)
  Caller 1 unblocks     Caller 2 unblocks
  (at different times)  (slower because cold path)
```

**No Ordering Guarantee Across Aggregates** — Caller 1 might return before Caller 2, or vice versa, depending on cold path latency.

---

## Testing Strategy

### Unit Tests for Each Component

**Router Tests**
```go
func TestRouter(t *testing.T) {
    r := NewRouter(8)

    // Same aggregate always routes to same shard
    shard1 := r.Route("order_123")
    shard2 := r.Route("order_123")
    assert.Equal(t, shard1, shard2)

    // Different aggregates distribute
    shards := make(map[int]bool)
    for i := 0; i < 100; i++ {
        s := r.Route(fmt.Sprintf("order_%d", i))
        shards[s] = true
    }
    // Should use multiple shards (not all in one)
    assert.Greater(t, len(shards), 1)
}
```

**Pool Tests**
```go
func TestShardPool(t *testing.T) {
    pool := NewShardPool(8, 100)  // 8 shards, queue depth 100

    // Should create 8 shards
    assert.Equal(t, len(pool.shards), 8)

    // Each shard queue should be ready
    for _, shard := range pool.shards {
        assert.NotNil(t, shard.commandChan)
    }
}
```

**Executor Tests**
```go
func TestExecutorValidationFails(t *testing.T) {
    executor := NewExecutor(mockEventStore, mockBus)

    // Command that fails validation
    cmd := &FailingValidationCmd{}
    err := executor.Execute(context.Background(), cmd, 1)

    assert.Equal(t, err, ErrValidation)
    // No event should be written
    assert.Equal(t, len(mockEventStore.events), 0)
}

func TestExecutorWriteFails(t *testing.T) {
    executor := NewExecutor(mockEventStore, mockBus)

    // Store that fails writes
    mockEventStore.writeError = errors.New("disk full")

    err := executor.Execute(context.Background(), cmd, 1)
    assert.Equal(t, err, ErrPipelineFailed)
}
```

---

### Integration Tests

**End-to-End Send()**
```go
func TestSendSuccess(t *testing.T) {
    store := asynx.NewMemoryStore()
    bus := asynx.NewChannelBus[Order]()
    processor := NewProcessor(store, bus, WithShards(4))

    cmd := CreateOrderCmd{OrderID: "order_1", Total: 99.99}
    err := processor.Send(context.Background(), cmd)

    assert.NoError(t, err)
    // Event should be in store
    state, _ := store.GetState("order_1")
    assert.NotNil(t, state)
}

func TestSendValidationError(t *testing.T) {
    store := asynx.NewMemoryStore()
    bus := asynx.NewChannelBus[Order]()
    processor := NewProcessor(store, bus)

    // Command that requires existing order
    cmd := ShipOrderCmd{OrderID: "non_existent"}
    err := processor.Send(context.Background(), cmd)

    assert.Equal(t, err, ErrValidation)
}
```

**Concurrent Sends to Same Aggregate**
```go
func TestConcurrentSameShard(t *testing.T) {
    store := asynx.NewMemoryStore()
    processor := NewProcessor(store, asynx.NewChannelBus[Order]())

    // Send 100 commands to same aggregate concurrently
    var wg sync.WaitGroup
    for i := 0; i < 100; i++ {
        wg.Add(1)
        go func(i int) {
            defer wg.Done()
            cmd := UpdateOrderCmd{OrderID: "order_1", Field: i}
            err := processor.Send(context.Background(), cmd)
            assert.NoError(t, err)
        }(i)
    }
    wg.Wait()

    // Final state should have all 100 versions applied
    state, _ := store.GetState("order_1")
    assert.Equal(t, state.Version, 100)
}
```

**Graceful Shutdown**
```go
func TestGracefulShutdown(t *testing.T) {
    processor := NewProcessor(store, bus)

    // Queue some commands
    go func() {
        for i := 0; i < 10; i++ {
            processor.Send(context.Background(), cmd)
            time.Sleep(10 * time.Millisecond)
        }
    }()

    time.Sleep(50 * time.Millisecond)

    // Initiate shutdown with timeout
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    err := processor.Shutdown(ctx)
    assert.NoError(t, err)

    // No more sends accepted
    err = processor.Send(context.Background(), cmd)
    assert.Equal(t, err, ErrShuttingDown)
}
```

---

## Performance Characteristics

### Send() Latency

**Best Case (warm path):**
```
Create envelope: ~1 µs
Route: ~1 µs (hash)
Queue send (channel): ~1 µs
Block on resultChan
  ↓
Get (snapshot + delta): ~10 ms
Validate: ~1 µs
Emit: ~1 µs
Write (serialize + append): ~50 ms (disk I/O)
Send result: ~1 µs
Total: ~60 ms (depends on storage)
```

**Worst Case (cold path):**
```
Get (full replay, 10000 events): ~500 ms
Validate: ~1 µs
Emit: ~1 µs
Write: ~50 ms
Total: ~550 ms (blocked on Get)
```

**Mitigation:** Call `Preload()` for hot aggregates at startup.

### Throughput

With **8 shards** and **2 ms per command**:
```
Throughput = 8 shards × (1000 ms / 2 ms) = 4000 commands/sec
```

Actually depends on:
- Number of shards (more = more parallelism)
- Storage latency (slower storage = lower throughput)
- Command complexity (longer validate/emit = lower throughput)
- Cold path hits (replay slows down Get)

### Memory Usage

With **QueueDepth=1000** and **8 shards**:
```
Queue memory per shard: 1000 envelopes × 200 bytes = 200 KB
Total: 8 shards × 200 KB = 1.6 MB (just queues)
Version map per shard: ~100 hot aggregates × 8 bytes = 800 bytes
Total: ~2 MB buffer memory
```

Actual memory includes:
- Command structs (varies)
- State structs (varies)
- Event objects (varies)

---

## Known Gotchas

### 1. Blind Retry on ErrPipelineFailed

**Wrong:**
```go
// This corrupts the stream!
cmd := CreateOrderCmd{...}
for i := 0; i < 3; i++ {
    err := processor.Send(ctx, cmd)
    if err == nil {
        break
    }
    // Just retrying, command not revalidated
}
```

**Right:**
```go
// Command is automatically revalidated on retry
cmd := CreateOrderCmd{...}
for i := 0; i < 3; i++ {
    err := processor.Send(ctx, cmd)
    if err == nil {
        break
    }
    if err == ErrPipelineFailed {
        // Caller must reload state and revalidate
        // Send() handles this automatically
        // (cmd is same struct, but state has changed)
        continue
    }
}
```

### 2. Context Cancellation Ambiguity

When ErrContextCancelled is returned, **is the event durable?**

```go
ctx, cancel := context.WithCancel(context.Background())
go func() {
    time.Sleep(100 * time.Millisecond)
    cancel()
}()

err := processor.Send(ctx, cmd)
if err == ErrContextCancelled {
    // Is the event durable?
    // Depends on timing:
    // - If cancelled before write: NO, event is lost
    // - If cancelled after write: YES, event is durable
    // CALLER CANNOT KNOW!
}
```

**Recommendation:** If ErrContextCancelled, assume event may or may not be durable. Check via `Get()` or check logs/metrics to determine state.

### 3. Queue Full Under Load

When `QueueDepth` is set and queue is full:

```go
processor := NewProcessor(store, bus, WithShardingOpts(ShardingOpts{
    Shards: 8,
    QueueDepth: 100,
}))

// If 800+ commands are in flight, queue is full
// New Send() calls get ErrQueueFull
// Caller must back off, not retry immediately
```

**Correct Handling:**
```go
for attempt := 0; attempt < 5; attempt++ {
    err := processor.Send(ctx, cmd)
    if err == nil {
        return nil
    }
    if err == ErrQueueFull {
        // Back off exponentially
        time.Sleep(time.Duration(math.Pow(2, float64(attempt))) * 100*time.Millisecond)
        continue
    }
    return err
}
```

### 4. Shutdown Timeout

If shutdown times out, **some work may not have completed:**

```go
ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
defer cancel()

err := processor.Shutdown(ctx)
if err == context.DeadlineExceeded {
    // Timeout exceeded
    // Some shard workers or projection callbacks may still be running
    // Operator must decide: force kill, wait longer, alert, etc.
    log.Printf("shutdown timeout: %v", err)
    os.Exit(1)  // Force kill (unsafe)
}
```

---

## Public API Design — Methods Across Modules

**Important:** Developers interact with a single public interface, but methods live in different modules internally:

```go
// Public interface (what developers see)
instance := asynx.New[Order]().WithEventStore(store).Build()

// Send & Shutdown → Processor module
instance.Send(ctx, cmd)
instance.Shutdown(ctx)

// Get, Exists, Preload → EventStore.Reader module
order, err := instance.Get(ctx, aggregateID)
exists, err := instance.Exists(ctx, aggregateID)
err := instance.Preload(ctx, aggregateID)

// Subscribe, Unsubscribe → Projection module (uses Bus interface)
id, err := instance.Subscribe(pattern, handler)
err := instance.Unsubscribe(id)

// Replay → EventStore.Replayer module
err := instance.Replay(ctx, aggregateID, fromVersion, toVersion, fn)
```

**Architecture:**
- Top-level `Instance[T]` type (in asynx package) aggregates and delegates to sub-modules
- Processor owns command execution pipeline and shard pool
- EventStore owns state hydration (reader), write boundary (writer), and event iteration (replayer)
- Projection owns subscription system and handler dispatch
- Bus owns event publication and subscriber routing

Developers never import sub-modules directly. All public methods are exposed through the Instance[T] interface.

---

## Worker Pool Implementation — Shard-Local Execution

The processor uses a **sharded worker pool** where each shard has **a fixed set of worker goroutines** instead of spawning one goroutine per command.

### Design: Worker Pool per Shard

Instead of:
```go
// ❌ Spawn goroutine per command (expensive under high concurrency)
go func() {
    resultChan <- executor.Execute(envelope.ctx, cmd, nextVersion)
}()
```

Use:
```go
// ✅ Fixed pool of workers per shard
type Shard[T any] struct {
    id              int
    commandChan     chan *CommandEnvelope[T]  // buffered
    workerCount     int                        // e.g., 1-4 workers per shard
    workers         []*Worker[T]               // pre-allocated
    // ...
}

// At shard startup:
for i := 0; i < s.workerCount; i++ {
    go s.workers[i].run(s.commandChan)  // Worker pulls from shared queue
}

// Each worker:
func (w *Worker[T]) run(commandChan chan *CommandEnvelope[T]) {
    for envelope := range commandChan {
        nextVersion := w.shard.versionMap[envelope.cmd.AggregateID()] + 1
        w.shard.versionMap[envelope.cmd.AggregateID()] = nextVersion

        err := executor.Execute(envelope.ctx, envelope.cmd, nextVersion)
        envelope.resultChan <- err
    }
}
```

### Benefits

- **Bounded goroutines**: Fixed number per shard (e.g., 8 shards × 2 workers = 16 goroutines max, vs. 80,000 with per-command spawning)
- **Better resource utilization**: Workers block naturally on channel receive (no context switching for idle workers)
- **Simpler lifecycle**: Workers are created at shard startup and destroyed at shutdown
- **Graceful degradation**: Under overload, commands queue up (bounded by QueueDepth), not goroutines spawn unbounded

### Configuration

```go
type ShardingOpts struct {
    Shards          int  // Number of shards (default 8)
    QueueDepth      int  // Per-shard buffer (default 0 = unbounded)
    WorkersPerShard int  // Workers per shard (default 1)
}
```

**Recommendation:** Start with `WorkersPerShard=1`. Each worker processes commands sequentially for that shard. If a single shard's commands are I/O-bound (waiting on storage), multiple workers per shard can help. For CPU-bound commands, 1 per shard is usually sufficient.

---

## Version Map Recovery — Handling Restart & Failures

### Scenario 1: Node Restart (Version Map Reset)

```
Node A: Processes order_123 → versionMap["order_123"] = 5
Node A crashes → memory lost, versionMap reset to 0

Node A restarts:
  New command for order_123 arrives
  nextVersion := versionMap["order_123"] + 1  // 0 + 1 = 1
  Try to write (order_123, version=1)
  But eventstore already has versions 1-5!
  Write fails: UNIQUENESS VIOLATION
  Executor returns ErrPipelineFailed

Caller retries Send() from scratch:
  eventstore.Get(order_123)  // Reloads state
    Replay events, discovers current version is 5
  Set versionMap["order_123"] = 5
  Next command uses version 6
```

**No data corruption.** The version map is a cache. If it's wrong (too low), the write fails, the caller retries, and the cache is corrected on the retry.

### Scenario 2: Validation Failure (Version Already Incremented)

```
Shard processes command for order_123:
  nextVersion := versionMap["order_123"] + 1  // 5 + 1 = 6
  versionMap["order_123"] = 6  // Updated early

  err := executor.Execute(...)
    Validate fails (command is invalid)
    Return ErrValidation immediately
    Never calls Write()

  versionMap["order_123"] is now 6
  But eventstore still has latest at version 5
  Next command for order_123 uses version 7
  Gap created in versionMap cache

Recovery:
  On next command failure that reloads from eventstore:
  Get(order_123) discovers version is 5
  Decrease versionMap["order_123"] = 5
```

**Design decision:** When validation fails, the version map stays incremented (creating a temporary "hole"). This is OK because:
1. The gap is only in the cache, not the eventstore
2. On next reload from eventstore (which happens on write failure), the cache is corrected
3. No permanent corruption

**Alternatively:** Rollback the version map immediately on validation failure:
```go
nextVersion := s.versionMap[aggregateID] + 1
s.versionMap[aggregateID] = nextVersion

err := executor.Execute(envelope.ctx, cmd, nextVersion)
if err == ErrValidation {
    // Rollback: validation failed, event not written
    s.versionMap[aggregateID] = nextVersion - 1
    envelope.resultChan <- err
}
```

**Recommended:** Use the rollback approach. It keeps the cache accurate and prevents gaps.

---

## Summary

The processor module uses a **sharded worker pool** with **channels-only synchronization**:

- **Router** hashes aggregate ID to shard
- **ShardPool** manages N independent shards
- **Shard workers** process commands sequentially
- **CommandExecutor** orchestrates the pipeline
- **Version management** is shard-local (no global lock)
- **Synchronization** via channels (stop signal, result channel)
- **Shutdown** is three-phase (stop intake, drain shards, drain bus)
- **No mutexes** between shards (eliminates deadlock risk)
- **Simple concurrency model** (select, channels, WaitGroup)

All commands for the same aggregate are processed **in order**. Commands for different aggregates are processed **in parallel**. Everything is built on Go's built-in concurrency primitives.

# Production Patterns

Patterns distilled from production consumers of asynx. All examples use a
generic `Order` aggregate; adapt names to your domain.

## Retrying on optimistic-concurrency conflicts

`Store.Append` enforces `(aggregateID, version)` uniqueness. When two writers
race on the same aggregate, the loser's append fails and asynx surfaces
`models.ErrPipelineFailed`. This is not a bug — it is the optimistic
concurrency control signal, and the correct response is to retry: the command
re-reads current state, re-validates, and appends at the new version.

Never retry `ErrValidation` — the command was rejected by domain rules and
will be rejected again.

```go
const maxRetries = 5

func sendWithRetry[T any](
	ctx context.Context,
	ax asynx.Asynx[T],
	cmd models.Command[T],
) (models.Event[T], error) {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		event, err := ax.Send(ctx, cmd)
		if err == nil {
			return event, nil
		}
		if !errors.Is(err, models.ErrPipelineFailed) {
			return event, err // validation and other errors: never retry
		}
		lastErr = err
	}
	var zero models.Event[T]
	return zero, lastErr
}
```

This matters most when background reactions send commands concurrently with
user-driven commands on the same aggregate (e.g. a drain goroutine marking an
order stale while a user updates it).

## Graceful shutdown with background reactions

Projection handlers often spawn long-running goroutines (executing a step,
calling an external system) that themselves send follow-up commands. Calling
`ax.Shutdown` while those goroutines are mid-flight makes their sends fail
with `ErrShuttingDown`.

Register background work in a `WaitGroup` guarded by a closed flag, and drain
before shutting asynx down:

```go
type runtime struct {
	ax          asynx.Asynx[Order]
	drainWg     sync.WaitGroup
	drainMu     sync.Mutex
	drainClosed bool
}

// tryAddDrain registers a background goroutine unless shutdown has begun.
func (r *runtime) tryAddDrain() bool {
	r.drainMu.Lock()
	defer r.drainMu.Unlock()
	if r.drainClosed {
		return false
	}
	r.drainWg.Add(1)
	return true
}

func (r *runtime) Shutdown(ctx context.Context) error {
	r.drainMu.Lock()
	r.drainClosed = true
	r.drainMu.Unlock()

	r.drainWg.Wait()           // let in-flight reactions finish their sends
	return r.ax.Shutdown(ctx)  // then stop command processing
}
```

Wire the whole thing to a deadline at the process level:

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
_ = r.Shutdown(ctx)
```

## Crash recovery via replay

Because state is fully reconstructable from events, startup recovery is a
read-and-reconcile pass: rehydrate each known aggregate, inspect the
reconstructed state, and issue commands to fix anything left in a transient
state by a crash.

```go
func recover(ctx context.Context, ax asynx.Asynx[Order], ids []string) error {
	for _, id := range ids {
		if err := ax.Preload(ctx, id); err != nil {
			return err
		}
		order, err := ax.Get(ctx, id)
		if errors.Is(err, models.ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if order.Status == "Processing" {
			// Crash mid-processing: reconcile with an explicit command so the
			// correction is itself an audited event.
			if _, err := ax.SendWait(ctx, RecoverInterruptedCmd{ID: id}); err != nil {
				return err
			}
		}
	}
	return nil
}
```

Use `SendWait` during recovery when subsequent startup steps depend on the
projections being up to date.

## Resilient projections

Projection handlers run asynchronously and may hit transient failures in
their own storage. Since the event is already durable, a handler can safely
retry its side effect without risking event loss:

```go
ax.Subscribe(asynx.Topic("order.created.*"), func(ctx context.Context, e models.Event[Order]) {
	saveWithRetry(ctx, e) // e.g. 3 attempts with backoff on transient DB errors
	hub.Broadcast(e)      // fan out to websockets, metrics, etc.
})
```

Combine with `models.WithFallback` for a last-resort handler and
`WithPanicHandler` on the builder for observability. Use
`WithPublishErrorHandler` to observe async publish failures — the event is
already stored, so these callbacks are purely for alerting.

## Implementing a durable Store

Guidelines proven out by SQL-backed implementations:

- **Enforce uniqueness in the database.** A `UNIQUE(aggregate_id, version)`
  constraint is the only synchronization asynx needs. Map constraint
  violations to `models.ErrPipelineFailed` so callers can apply the retry
  pattern above.
- **Keep `Delete` idempotent.** `Forget` relies on it; deleting a missing
  aggregate must not error.
- **Serialize writes where the backend requires it.** For SQLite, set
  `db.SetMaxOpenConns(1)` (or use a write mutex) to avoid `SQLITE_BUSY`.
- **Expose a `Close()` beyond the Store interface** if the backend needs
  teardown (e.g. WAL checkpointing), and call it after `ax.Shutdown` returns.

## Implementing a durable SnapshotStore

`models.SnapshotStore` is a separate, required interface (`Put`/`Get`/`Delete`)
— not a second `Store`. Guidelines:

- **`Put` must be a true upsert, keyed on `aggregate_id` alone.** A
  `PRIMARY KEY (aggregate_id, version)` table reintroduces the unbounded
  growth this interface exists to avoid — see
  [docs/spec/store.md § SnapshotStore](./store.md#snapshotstore) for the
  schema and reference SQL.
- **The optional monotonicity guard (`WHERE excluded.version > snapshots.version`)
  is a nice-to-have, not a correctness requirement.** Asynx tolerates
  last-write-wins on snapshots — an older snapshot overwriting a newer one
  just costs extra replay on the next read, never incorrect state.
- **Losing data is safe; corrupting data is not.** A `SnapshotStore` may be
  backed by something without durability guarantees (Redis without
  persistence, an LRU cache) — every snapshot is rebuildable from the event
  stream. What must not happen is `Get` returning a snapshot for the wrong
  aggregate or a truncated blob without an error.

## Testing with a real asynx instance

Prefer exercising real event sourcing over mocking `Asynx[T]`. Build an
instance over the bundled in-memory store with small shard counts, and always
shut it down in cleanup:

```go
func newTestAsynx(t *testing.T) asynx.Asynx[Order] {
	t.Helper()
	ax, err := asynx.New[Order]().
		WithEventStore(store.New()).
		WithSnapshotStore(store.NewSnapshots()).
		WithShardingOpts(asynx.ShardingOpts{Shards: 4, QueueDepth: 100}).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = ax.Shutdown(ctx)
	})
	return ax
}
```

Use `ax.WaitPublish()` after sends before asserting on projection side
effects, and `store.Memory.SetError` to inject one-shot append failures when
testing error paths. Keep the read/query side behind its own interface so
query tests can mock it while command tests run the real pipeline.

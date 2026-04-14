# Forget as a Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `Forget(ctx, aggregateID)` to Asynx — writes a tombstone event, notifies ForgetHandlers synchronously, then deletes all events and snapshots for the aggregate.

**Architecture:** `forgetCommand[T]` implements `models.Command[T]` and flows through the normal `SendWait` pipeline (shard-serialized, tombstone written, handlers called). After `SendWait` returns, `EventStore.Delete` wipes both event and snapshot streams. No shard pool changes are needed.

**Tech Stack:** Go 1.26, stdlib only (`context`, `errors`, `fmt`, `sync`). Run tests with `make test` (`go test -race ./...`).

---

## File Map

| File | Change |
|---|---|
| `models/errors.go` | Add `ErrForgetFailed` sentinel |
| `models/handlers.go` | Add `ForgetHandler[T]` named type |
| `models/store.go` | Add `Delete` to `Store` interface |
| `internal/store/memory.go` | Implement `Delete` |
| `internal/store/memory_test.go` | Test `Delete` |
| `internal/mocks/store.go` | Add `Delete` to `Store`, `ErrStore`, `CorruptBlobStore` |
| `internal/eventstore/eventstore.go` | Add `Delete` method |
| `internal/eventstore/eventstore_test.go` | Test `EventStore.Delete` |
| `forget.go` (new) | `forgetCommand[T]`, `asynxImpl.Forget`, `asynxImpl.OnForget` |
| `asynx.go` | Add `Forget` and `OnForget` to `Asynx[T]` interface |
| `asynx_test.go` | Integration tests for Forget, OnForget |
| `builder.go` | Add `forgetHandlers` field, `WithForgetHandler`, update `Build` |
| `builder_test.go` | Test `WithForgetHandler` |

---

## Task 1: Foundation types — `ErrForgetFailed` and `ForgetHandler[T]`

**Files:**
- Modify: `models/errors.go`
- Modify: `models/handlers.go`

- [ ] **Step 1: Add `ErrForgetFailed` to `models/errors.go`**

Open `models/errors.go`. Append one line to the `var` block:

```go
var (
	ErrNotFound            = errors.New("asynx: aggregate not found")
	ErrValidation          = errors.New("asynx: validation failed")
	ErrPipelineFailed      = errors.New("asynx: pipeline failed")
	ErrQueueFull           = errors.New("asynx: queue full")
	ErrShuttingDown        = errors.New("asynx: shutting down")
	ErrAlreadyShuttingDown = errors.New("asynx: already shutting down")
	ErrContextCancelled    = errors.New("asynx: context cancelled")
	ErrBusClosed           = errors.New("asynx: bus closed")
	ErrNilHandler          = errors.New("asynx: handler is nil")
	ErrEmptyPattern        = errors.New("asynx: pattern is empty")
	ErrMissingEventStore   = errors.New("asynx: event store is required")
	ErrForgetFailed        = errors.New("asynx: forget failed")
)
```

- [ ] **Step 2: Add `ForgetHandler[T]` to `models/handlers.go`**

Append after the existing `PublishErrorHandler[T]` type:

```go
// ForgetHandler is called when an aggregate is forgotten.
// It receives the tombstone event; Event.Aggregate holds the aggregate's last known state.
type ForgetHandler[T any] func(context.Context, Event[T])
```

- [ ] **Step 3: Verify it compiles**

```bash
go build ./models/...
```

Expected: no output (success).

- [ ] **Step 4: Commit**

```bash
git add models/errors.go models/handlers.go
git commit -m "feat(models): add ErrForgetFailed sentinel and ForgetHandler type"
```

---

## Task 2: `Delete` on `models.Store` + `store.Memory` + mock stores

**Files:**
- Modify: `models/store.go`
- Modify: `internal/store/memory.go`
- Modify: `internal/store/memory_test.go`
- Modify: `internal/mocks/store.go`

- [ ] **Step 1: Write the failing tests in `internal/store/memory_test.go`**

Append to the end of `internal/store/memory_test.go`:

```go
// --- Delete ---

func TestDelete_RemovesAllEntries(t *testing.T) {
	s := New()
	ctx := context.Background()

	s.Append(ctx, "agg1", 1, []byte("v1")) //nolint:errcheck
	s.Append(ctx, "agg1", 2, []byte("v2")) //nolint:errcheck

	if err := s.Delete(ctx, "agg1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	blobs, err := s.ReadFrom(ctx, "agg1", 1)
	if err != nil {
		t.Fatalf("ReadFrom after Delete: %v", err)
	}
	if len(blobs) != 0 {
		t.Errorf("expected 0 blobs after Delete, got %d", len(blobs))
	}
}

func TestDelete_NonExistentAggregate_NoError(t *testing.T) {
	s := New()
	if err := s.Delete(context.Background(), "ghost"); err != nil {
		t.Errorf("Delete on non-existent aggregate: %v (want nil)", err)
	}
}

func TestDelete_DoesNotAffectOtherAggregates(t *testing.T) {
	s := New()
	ctx := context.Background()

	s.Append(ctx, "agg1", 1, []byte("v1")) //nolint:errcheck
	s.Append(ctx, "agg2", 1, []byte("v2")) //nolint:errcheck

	s.Delete(ctx, "agg1") //nolint:errcheck

	blobs, err := s.ReadFrom(ctx, "agg2", 1)
	if err != nil {
		t.Fatalf("ReadFrom agg2: %v", err)
	}
	if len(blobs) != 1 {
		t.Errorf("expected 1 blob for agg2, got %d", len(blobs))
	}
}

func TestDelete_CancelledContext_ReturnsError(t *testing.T) {
	s := New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := s.Delete(ctx, "agg1"); err == nil {
		t.Error("expected error for cancelled context, got nil")
	}
}
```

- [ ] **Step 2: Run the tests to confirm they fail**

```bash
go test ./internal/store/... -run TestDelete -v
```

Expected: compilation failure — `s.Delete undefined`.

- [ ] **Step 3: Add `Delete` to the `models.Store` interface**

Replace the full contents of `models/store.go`:

```go
package models

import "context"

type Store interface {
	// Append enforces (aggregateID, version) uniqueness — the sole coordination
	// mechanism for correctness across concurrent writers.
	Append(
		ctx context.Context,
		aggregateID string,
		version int64,
		data []byte,
	) error

	ReadFrom(
		ctx context.Context,
		aggregateID string,
		fromVersion int64,
	) ([][]byte, error)

	ReadRange(
		ctx context.Context,
		aggregateID string,
		fromVersion int64,
		count int64,
	) ([][]byte, error)

	// Count returns the number of entries with version >= fromVersion.
	Count(
		ctx context.Context,
		aggregateID string,
		fromVersion int64,
	) (int64, error)

	// Delete removes all records for the given aggregateID.
	// Idempotent — deleting a non-existent aggregateID is not an error.
	Delete(
		ctx context.Context,
		aggregateID string,
	) error
}
```

- [ ] **Step 4: Implement `Delete` in `internal/store/memory.go`**

Append this method after the `Count` method:

```go
func (s *Memory) Delete(ctx context.Context, aggregateID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.streams, aggregateID)
	return nil
}
```

- [ ] **Step 5: Add `Delete` to the three mock stores in `internal/mocks/store.go`**

Append these methods to the file. Add after each existing `Count` method — one per type:

```go
func (s *Store) Delete(_ context.Context, _ string) error {
	return nil
}
```

```go
func (e *ErrStore) Delete(_ context.Context, _ string) error {
	return e.Err
}
```

```go
func (c *CorruptBlobStore) Delete(_ context.Context, _ string) error {
	return nil
}
```

- [ ] **Step 6: Run the tests to confirm they pass**

```bash
go test -race ./internal/store/... -run TestDelete -v
```

Expected: all four `TestDelete_*` tests PASS.

- [ ] **Step 7: Confirm nothing else broke**

```bash
make test
```

Expected: all tests pass.

- [ ] **Step 8: Commit**

```bash
git add models/store.go internal/store/memory.go internal/store/memory_test.go internal/mocks/store.go
git commit -m "feat(store): add Delete to models.Store interface and implement in Memory"
```

---

## Task 3: `EventStore.Delete`

**Files:**
- Modify: `internal/eventstore/eventstore.go`
- Modify: `internal/eventstore/eventstore_test.go`

**Key context:** Events are stored under the key `"events:"+aggregateID` and snapshots under `"snapshots:"+aggregateID` in the backing `models.Store`. `EventStore.Delete` must call both.

- [ ] **Step 1: Write the failing test in `internal/eventstore/eventstore_test.go`**

Append to the end of `internal/eventstore/eventstore_test.go`:

```go
// --- Delete ---

func TestDelete_AfterWrite_GetReturnsErrNotFound(t *testing.T) {
	s := store.New()
	es := New[order](s, s, nil, 1, nil)
	ctx := context.Background()

	if _, err := es.Write(ctx, cmd(order{ID: "1", Status: "Pending", Total: 100})); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if err := es.Delete(ctx, "agg1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err := es.Get(ctx, "agg1")
	if !errors.Is(err, asynxmd.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after Delete, got %v", err)
	}
}

func TestDelete_RemovesSnapshot(t *testing.T) {
	s := store.New()
	es := New[order](s, s, nil, 1, nil)
	ctx := context.Background()

	if _, err := es.Write(ctx, cmdSnap(order{ID: "1", Status: "Snapped", Total: 50})); err != nil {
		t.Fatalf("Write with snapshot: %v", err)
	}

	if err := es.Delete(ctx, "agg1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err := es.Get(ctx, "agg1")
	if !errors.Is(err, asynxmd.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after Delete (snapshot path), got %v", err)
	}
}

func TestDelete_NonExistentAggregate_NoError(t *testing.T) {
	es := New[order](store.New(), store.New(), nil, 1, nil)
	if err := es.Delete(context.Background(), "ghost"); err != nil {
		t.Errorf("Delete on non-existent aggregate: %v (want nil)", err)
	}
}
```

- [ ] **Step 2: Run to confirm they fail**

```bash
go test ./internal/eventstore/... -run TestDelete -v
```

Expected: compilation failure — `es.Delete undefined`.

- [ ] **Step 3: Implement `EventStore.Delete` in `internal/eventstore/eventstore.go`**

Append this method after `Replay`:

```go
// Delete removes all events and snapshots for the aggregate from the backing stores.
// Idempotent — deleting a non-existent aggregate is not an error.
func (es *EventStore[T]) Delete(
	ctx context.Context,
	aggregateID string,
) error {
	if err := es.writer.EventStore().Delete(ctx, "events:"+aggregateID); err != nil {
		return err
	}
	return es.writer.SnapshotStore().Delete(ctx, "snapshots:"+aggregateID)
}
```

- [ ] **Step 4: Expose `EventStore()` and `SnapshotStore()` accessors on `Writer`**

Open `internal/eventstore/writer/writer.go`. The `Writer[T]` struct holds `es` and `ss` fields. Add two accessor methods:

```go
func (w *Writer[T]) EventStore() asynxmd.Store    { return w.es }
func (w *Writer[T]) SnapshotStore() asynxmd.Store { return w.ss }
```

> Check the actual field names in `internal/eventstore/writer/writer.go` before adding — they may differ from `es`/`ss`. Adjust accordingly.

- [ ] **Step 5: Run the tests to confirm they pass**

```bash
go test -race ./internal/eventstore/... -run TestDelete -v
```

Expected: all three `TestDelete_*` tests PASS.

- [ ] **Step 6: Confirm nothing else broke**

```bash
make test
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add internal/eventstore/eventstore.go internal/eventstore/eventstore_test.go internal/eventstore/writer/writer.go
git commit -m "feat(eventstore): add Delete — erases events and snapshots for an aggregate"
```

---

## Task 4: `forgetCommand[T]`, `Forget`, and `OnForget`

**Files:**
- Create: `forget.go`
- Modify: `asynx.go`
- Modify: `asynx_test.go`

- [ ] **Step 1: Write the failing integration tests in `asynx_test.go`**

Append to the end of `asynx_test.go`:

```go
// --- Forget ---

func TestForget_AggregateDoesNotExist_ReturnsErrValidation(t *testing.T) {
	instance := newInstance(t)
	err := instance.Forget(context.Background(), "order-missing")
	if !errors.Is(err, models.ErrValidation) {
		t.Fatalf("expected ErrValidation, got %v", err)
	}
}

func TestForget_ErasesAggregate(t *testing.T) {
	instance := newInstance(t)
	ctx := context.Background()

	if _, err := instance.Send(ctx, mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if err := instance.Forget(ctx, "order-1"); err != nil {
		t.Fatalf("Forget: %v", err)
	}

	_, err := instance.Get(ctx, "order-1")
	if !errors.Is(err, models.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after Forget, got %v", err)
	}
}

func TestForget_CallsOnForgetHandler_WithLastState(t *testing.T) {
	instance := newInstance(t)
	ctx := context.Background()

	if _, err := instance.Send(ctx, mocks.CreateOrderCmd{ID: "order-1", Total: 99.0}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	var gotEvent models.Event[mocks.Order]
	if _, err := instance.OnForget(func(_ context.Context, e models.Event[mocks.Order]) {
		gotEvent = e
	}); err != nil {
		t.Fatalf("OnForget: %v", err)
	}

	if err := instance.Forget(ctx, "order-1"); err != nil {
		t.Fatalf("Forget: %v", err)
	}

	if gotEvent.AggregateID != "order-1" {
		t.Errorf("AggregateID = %q, want order-1", gotEvent.AggregateID)
	}
	if gotEvent.EventName != "asynx.aggregate.forget" {
		t.Errorf("EventName = %q, want asynx.aggregate.forget", gotEvent.EventName)
	}
	if gotEvent.Aggregate.Total != 99.0 {
		t.Errorf("Aggregate.Total = %v, want 99.0", gotEvent.Aggregate.Total)
	}
}

func TestForget_AfterShutdown_ReturnsErrShuttingDown(t *testing.T) {
	s := store.New()
	instance, err := asynx.New[mocks.Order]().WithEventStore(s).Build()
	if err != nil {
		t.Fatal(err)
	}
	instance.Shutdown(context.Background())

	if err := instance.Forget(context.Background(), "order-1"); !errors.Is(err, models.ErrShuttingDown) {
		t.Fatalf("expected ErrShuttingDown, got %v", err)
	}
}

func TestForget_SerializedAfterSend(t *testing.T) {
	instance := newInstance(t)
	ctx := context.Background()

	if _, err := instance.Send(ctx, mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}); err != nil {
		t.Fatalf("initial Send: %v", err)
	}

	// Enqueue an update and a Forget — Forget must see the updated state.
	updated := mocks.Order{ID: "order-1", Total: 200.0, Status: "Updated"}
	instance.Send(ctx, mocks.UpdateOrderCmd{ID: "order-1", NewState: updated}) //nolint:errcheck

	var gotTotal float64
	instance.OnForget(func(_ context.Context, e models.Event[mocks.Order]) { //nolint:errcheck
		gotTotal = e.Aggregate.Total
	})

	if err := instance.Forget(ctx, "order-1"); err != nil {
		t.Fatalf("Forget: %v", err)
	}

	if gotTotal != 200.0 {
		t.Errorf("Forget saw Total=%v, want 200.0 (Send should have completed first)", gotTotal)
	}
}

func TestOnForget_Unsubscribe_StopsHandler(t *testing.T) {
	instance := newInstance(t)
	ctx := context.Background()

	if _, err := instance.Send(ctx, mocks.CreateOrderCmd{ID: "order-1", Total: 100.0}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	var called int
	subID, err := instance.OnForget(func(_ context.Context, _ models.Event[mocks.Order]) {
		called++
	})
	if err != nil {
		t.Fatalf("OnForget: %v", err)
	}

	if err := instance.Unsubscribe(subID); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}

	if err := instance.Forget(ctx, "order-1"); err != nil {
		t.Fatalf("Forget: %v", err)
	}

	if called != 0 {
		t.Errorf("handler called %d times after Unsubscribe, want 0", called)
	}
}
```

Make sure `"errors"` is in the import block of `asynx_test.go`. If it isn't, add it.

- [ ] **Step 2: Run to confirm they fail**

```bash
go test ./... -run TestForget -v
```

Expected: compilation failure — `instance.Forget undefined`, `instance.OnForget undefined`.

- [ ] **Step 3: Add `Forget` and `OnForget` to the `Asynx[T]` interface in `asynx.go`**

Add after `SendWait`:

```go
// Forget writes a tombstone event for the aggregate, notifies all ForgetHandlers
// synchronously, then erases all events, snapshots, and cached state.
// Returns ErrValidation if the aggregate does not exist.
Forget(
    ctx context.Context,
    aggregateID string,
) error

// OnForget registers a handler invoked when any aggregate is forgotten.
// The handler receives the tombstone event; Event.Aggregate holds the last known state.
// Returns a subscription ID that can be passed to Unsubscribe.
OnForget(
    fn models.ForgetHandler[T],
) (string, error)
```

- [ ] **Step 4: Create `forget.go` with `forgetCommand[T]` and method implementations**

```go
package asynx

import (
	"context"
	"fmt"

	"github.com/char2cs/asynx/models"
)

type forgetCommand[T any] struct {
	aggregateID string
	last        *T
}

func (c *forgetCommand[T]) AggregateID() string { return c.aggregateID }
func (c *forgetCommand[T]) EventName() string    { return "asynx.aggregate.forget" }
func (c *forgetCommand[T]) ShouldSnapshot() bool { return false }

func (c *forgetCommand[T]) Validate(current *T) error {
	if current == nil {
		return fmt.Errorf("%w: aggregate %s not found", models.ErrValidation, c.aggregateID)
	}
	c.last = current
	return nil
}

func (c *forgetCommand[T]) EmitEvent(_ *T) T { return *c.last }

func (i *asynxImpl[T]) Forget(ctx context.Context, aggregateID string) error {
	_, err := i.proc.SendWait(ctx, &forgetCommand[T]{aggregateID: aggregateID})
	if err != nil {
		return err
	}
	if err := i.es.Delete(ctx, aggregateID); err != nil {
		return fmt.Errorf("%w: %w", models.ErrForgetFailed, err)
	}
	return nil
}

func (i *asynxImpl[T]) OnForget(fn models.ForgetHandler[T]) (string, error) {
	return i.bus.Subscribe("asynx.aggregate.forget", models.ProjectionHandler[T](fn))
}
```

- [ ] **Step 5: Run the tests to confirm they pass**

```bash
go test -race ./... -run TestForget -v
go test -race ./... -run TestOnForget -v
```

Expected: all tests PASS.

- [ ] **Step 6: Run the full suite**

```bash
make test
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add forget.go asynx.go asynx_test.go
git commit -m "feat: add Forget and OnForget — tombstone write, handler notification, aggregate erasure"
```

---

## Task 5: `WithForgetHandler` on `Builder`

**Files:**
- Modify: `builder.go`
- Modify: `builder_test.go`

- [ ] **Step 1: Write the failing test in `builder_test.go`**

Append to the end of `builder_test.go`:

```go
func TestWithForgetHandler_IsCalledOnForget(t *testing.T) {
	var gotTotal float64

	instance, err := asynx.New[mocks.Order]().
		WithEventStore(store.New()).
		WithForgetHandler(func(_ context.Context, e models.Event[mocks.Order]) {
			gotTotal = e.Aggregate.Total
		}).
		Build()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { instance.Shutdown(context.Background()) })

	ctx := context.Background()
	if _, err := instance.Send(ctx, mocks.CreateOrderCmd{ID: "order-1", Total: 77.0}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := instance.Forget(ctx, "order-1"); err != nil {
		t.Fatalf("Forget: %v", err)
	}

	if gotTotal != 77.0 {
		t.Errorf("ForgetHandler got Total=%v, want 77.0", gotTotal)
	}
}
```

Make sure `builder_test.go` imports `"github.com/char2cs/asynx/internal/store"` alongside the existing imports. Check the top of the file — if `store` isn't imported, add it.

- [ ] **Step 2: Run to confirm it fails**

```bash
go test ./... -run TestWithForgetHandler -v
```

Expected: compilation failure — `WithForgetHandler undefined`.

- [ ] **Step 3: Add `forgetHandlers` field and `WithForgetHandler` method to `builder.go`**

Add the field to `Builder[T]`:

```go
type Builder[T any] struct {
	eventStore          models.Store
	snapshotStore       models.Store
	bus                 models.Bus[T]
	shardingOpts        ShardingOpts
	schemaVersion       int
	upcasters           map[int]models.Upcaster
	panicHandler        models.PanicHandler[T]
	corruptionHook      func(error)
	publishErrorHandler models.PublishErrorHandler[T]
	forgetHandlers      []models.ForgetHandler[T]
}
```

Add the builder method after `WithPublishErrorHandler`:

```go
// WithForgetHandler registers a ForgetHandler at build time.
// Equivalent to calling OnForget after Build. Multiple calls register multiple handlers.
func (b *Builder[T]) WithForgetHandler(fn models.ForgetHandler[T]) *Builder[T] {
	b.forgetHandlers = append(b.forgetHandlers, fn)
	return b
}
```

- [ ] **Step 4: Register forget handlers in `Build()`**

In `builder.go`, find the `Build()` method. After the `asynxImpl` is constructed and before the `return`, add:

```go
for _, fn := range b.forgetHandlers {
    if _, err := instance.OnForget(fn); err != nil {
        return nil, err
    }
}
```

The full return block in `Build()` becomes:

```go
instance := &asynxImpl[T]{
    proc: proc,
    es:   es,
    bus:  activeBus,
}

for _, fn := range b.forgetHandlers {
    if _, err := instance.OnForget(fn); err != nil {
        return nil, err
    }
}

return instance, nil
```

- [ ] **Step 5: Run the test to confirm it passes**

```bash
go test -race ./... -run TestWithForgetHandler -v
```

Expected: PASS.

- [ ] **Step 6: Run the full suite**

```bash
make test
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add builder.go builder_test.go
git commit -m "feat(builder): add WithForgetHandler — registers ForgetHandler at build time"
```

---

## Self-Review

**Spec coverage:**

| Spec requirement | Task |
|---|---|
| `Forget(ctx, aggregateID) error` on `Asynx[T]` | Task 4 |
| `OnForget(fn ForgetHandler[T]) (string, error)` on `Asynx[T]` | Task 4 |
| `WithForgetHandler` on `Builder[T]` | Task 5 |
| `ForgetHandler[T]` named type in `models/handlers.go` | Task 1 |
| `ErrForgetFailed` in `models/errors.go` | Task 1 |
| `Delete` on `models.Store` | Task 2 |
| `EventStore.Delete` (events + snapshots) | Task 3 |
| `forgetCommand[T]` writes tombstone `"asynx.aggregate.forget"` | Task 4 |
| `Validate` returns `ErrValidation` if aggregate missing | Task 4 |
| `EmitEvent` returns last known state | Task 4 |
| Uses `SendWait` for shard serialization + sync handler dispatch | Task 4 |
| `ErrValidation` if aggregate doesn't exist | Task 4 — `TestForget_AggregateDoesNotExist_ReturnsErrValidation` |
| `ErrShuttingDown` during shutdown | Task 4 — `TestForget_AfterShutdown_ReturnsErrShuttingDown` |
| `ForgetHandler` receives last state | Task 4 — `TestForget_CallsOnForgetHandler_WithLastState` |
| Shard serialization with Send | Task 4 — `TestForget_SerializedAfterSend` |
| `Unsubscribe` stops handler | Task 4 — `TestOnForget_Unsubscribe_StopsHandler` |
| `Delete` removes events and snapshots | Task 3 — `TestDelete_AfterWrite_GetReturnsErrNotFound`, `TestDelete_RemovesSnapshot` |
| Delete on non-existent is no-op | Tasks 2 and 3 |

**Placeholder scan:** None found. All steps contain complete code.

**Type consistency:**
- `ForgetHandler[T]` defined in Task 1, used in Tasks 4 and 5 ✓
- `ErrForgetFailed` defined in Task 1, used in Task 4 (`forget.go`) ✓
- `models.Store.Delete` defined in Task 2, implemented in Task 2, called in Task 3 ✓
- `EventStore.Delete` defined in Task 3, called in Task 4 (`forget.go`) ✓
- `forgetCommand[T]` defined and used only in Task 4 ✓
- `asynxImpl.OnForget` defined in Task 4, called in Task 5 ✓

**Note on Task 3, Step 4:** Before adding `EventStore()` and `SnapshotStore()` accessors to `Writer`, read `internal/eventstore/writer/writer.go` to verify the actual field names for the event store and snapshot store. Adjust the accessor bodies if the fields are named differently.

# EmitWait Design

**Date:** 2026-04-05
**Status:** Approved
**Branch:** feature/wait-4-projection

## Problem

`Send` is fire-and-forget for event publishing. After it returns, the event is durably written but projection handlers run asynchronously — callers have no way to know when projections are up to date, or to get the `Event[T]` that was produced.

The workaround today (`WaitPublish` + `WaitForHandlers`) is test-only, drains *all* in-flight work globally, and doesn't return the event.

## Goal

Add `EmitWait` — a first-class production API that:
1. Sends a command and blocks until the event is written **and** all projection handlers for that event have completed.
2. Returns the resulting `Event[T]` (same value projection handlers receive).
3. Does not affect `Send` behavior in any way.

## Public API

```go
type Asynx[T any] interface {
    // ... existing methods unchanged ...

    // EmitWait sends cmd, blocks until the event is durably written and all
    // matching projection handlers have finished, then returns the event.
    // Handler panics and timeouts are still handled by PanicHandler/TimeoutHandler;
    // they do not affect the returned error.
    EmitWait(ctx context.Context, cmd models.Command[T]) (models.Event[T], error)
}
```

Error semantics are identical to `Send`: validation errors, pipeline failures, and context cancellation all surface the same way.

## Bus Interface Extension

```go
type Bus[T any] interface {
    // ... existing methods unchanged ...

    // PublishSync fires all matching handlers synchronously — it blocks until
    // every handler goroutine triggered by this specific event has completed.
    // It does NOT wait for handlers from other concurrent events.
    PublishSync(ctx context.Context, event Event[T]) error
}
```

`ChannelBus` implements `PublishSync` by using a per-publish local `sync.WaitGroup` — `Add(n)` before spawning handlers, each goroutine calls `Done()`, method waits before returning. Shared subscription-matching logic is extracted into a helper used by both `Publish` and `PublishSync`.

## Internal Pipeline Changes

Six mechanical changes that thread `Event[T]` back through the pipeline and introduce the `WaitHandlers` flag:

### `internal/processor/models/envelope.go`
- Add `CommandResult[T]` struct: `{ Event Event[T]; Err error }`
- `CommandEnvelope.ResultChan` changes from `chan error` → `chan CommandResult[T]`
- Add `WaitHandlers bool` field to `CommandEnvelope`

### `internal/processor/exec/exec.go`
- `Execute` becomes `Execute(ctx, cmd, nextVersion, waitHandlers bool) (Event[T], error)`
- `waitHandlers=false` → `publishAsync` (unchanged current behavior)
- `waitHandlers=true` → `bus.PublishSync` (blocks until all handlers for this event complete)

### `internal/processor/pool/shard.go`
- `executeJob` passes `envelope.WaitHandlers` to `executor.Execute` and reads back the event
- `sendResult` sends `CommandResult[T]` instead of bare `error`

### `internal/processor/processor.go`
- `sendAndWait` returns `(Event[T], error)` — reads `CommandResult[T]` from the result channel
- `Send` calls `sendAndWait`, discards the event, returns only `error` — no behavioral change
- New `EmitWait` method: creates envelope with `WaitHandlers=true`, calls `sendAndWait`, returns `(Event[T], error)`

### `asynx.go`
- Add `EmitWait` to the `Asynx[T]` interface
- Implement on `asynxImpl` by delegating to `proc.EmitWait`

### `internal/mocks/bus.go`
- Add `PublishSync` to the mock bus to satisfy the updated `Bus[T]` interface

## Testing

- All existing `Send` tests pass unchanged — `Send` behavior is identical.
- New tests for `EmitWait`:
  - Returns the correct `Event[T]` (aggregate, previous aggregate, event name, version).
  - Handlers are guaranteed complete when `EmitWait` returns (assert side effects without `WaitPublish`).
  - Context cancellation while waiting returns `ErrContextCancelled`.
  - Validation failure returns `ErrValidation`, no event published.

## What This Does Not Change

- `Send` — identical behavior, no signature change.
- `WaitPublish` / `WaitForHandlers` — remain as test helpers, unchanged.
- Subscription/unsubscription — unchanged.
- Shard routing, version management, shutdown — unchanged.

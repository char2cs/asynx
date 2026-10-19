# Dispatcher Specification

## Overview

The dispatcher is the component that carries committed events to handlers. It owns per-aggregate ordering, the durable retry loop, dead-lettering, and the park state of an aggregate whose delivery gave up. The processor hands it every event after the commit; the dispatcher runs the live subscribers (through the `Bus[T]`) and the durable subscribers, and calls the `Ack` it was given once every durable handler has succeeded.

`models.Dispatcher[T]` is a public interface and `Builder.WithDispatcher` replaces the implementation. **The library ships exactly one implementation: the in-memory default. It contains no broker client (no Kafka, NATS, SQS or similar) and none will be added; a broker-backed dispatcher is code you write against this contract.**

What does not move with the dispatcher:

- The **delivery log** stays in the `Store` (`PendingDeliveries`, `AckDelivery`). The processor reads it at `Build()` and in `ResumeDeliveries` and enqueues what it finds as `DispatchRedelivery` requests.
- The **aggregate lock** and command execution stay in the processor. The dispatcher never decides who may write.
- **Recovery and resume orchestration** (which aggregates to resume, in which order, under which lock) stays in the processor. The dispatcher only answers `ResumeUndelivered` and `Park`.

## Scope: one process

The aggregate lock, command execution and the delivery log are single-process, as described in [guarantees.md](./guarantees.md). A custom dispatcher changes where handlers run and how events travel, not who may write. Do not use a dispatcher to make several processes share one store: the exclusivity and ordering guarantees still hold only per process.

## The interface

```go
type Dispatcher[T any] interface {
    Enqueue(ctx context.Context, request DispatchRequest[T]) (Ticket, error)
    TicketFor(aggregateID string, version int64) (Ticket, bool)
    Unrun(aggregateID string, version int64) error
    Park(aggregateID string, cause error, undelivered ...int64)
    Supersede(aggregateID string)
    Parks() map[string]uint64
    ResumeUndelivered(aggregateID string, generation uint64) (lifted bool, undelivered []int64)
    Inspect(aggregateID string) DeliveryStatus
    InHandler(ctx context.Context, aggregateID string) bool
    Interrupt()
    Interrupted() <-chan struct{}
    Close(ctx context.Context) error
    WaitIdle()
}

type DispatchRequest[T any] struct {
    Event Event[T]
    Ack   func(context.Context) error // non-nil: durable; called once every durable handler succeeded
    Kind  DispatchKind
}

type DispatchKind int // DispatchEvent, DispatchRedelivery, DispatchTombstone

type DeliveryStatus struct {
    Parked      bool
    Generation  uint64  // changes each time a park is imposed; zero when not parked
    Cause       error   // why it was parked; nil when not parked (and for a park imposed without one)
    Undelivered []int64 // ascending versions no handler took; nil when not parked
}

type DispatcherConfig[T any] struct {
    Bus             Bus[T]
    DurableSubs     []DurableSub[T]
    OnPublishError  PublishErrorHandler[T]
    OnDeliveryError DeliveryErrorHandler[T]
    OnPanic         PanicHandler[T]
    DeadLetter      DeadLetterHandler[T]
    RetryInitial    time.Duration
    RetryMax        time.Duration
    MaxAttempts     int
}

type DispatcherFactory[T any] func(DispatcherConfig[T]) (Dispatcher[T], error)
```

`Ticket` (`Settled`, `Final`, `Err`, `Unrun`) and `DurableSub[T]` live in `models`.

`DispatcherConfig` carries **final values**. The processor fills the defaults where it builds the config, before calling the factory: 20 attempts and a backoff of 100 ms doubling to 30 s, or whatever `WithDeliveryMaxAttempts` and `WithDeliveryRetry` set (non-positive settings are dropped). An implementation never needs to know them. `Bus` is the live-subscriber transport and may be nil, in which case no live subscriber is reached. The idle timeout and worker goroutine counts of the default are not part of the config: they are the default's own business.

### Kinds of request

| Kind | Sent by | Live subscribers (`Bus.PublishSync`) | Durable subscribers |
|---|---|---|---|
| `DispatchEvent` | `Send`, `SendWait` | yes | the matching ones, if `Ack != nil` |
| `DispatchRedelivery` | `Build()` recovery, `ResumeDeliveries` | never | the matching ones, if `Ack != nil`; every one if the event is named `models.ForgetEventName` (with or without an `Ack`) |
| `DispatchTombstone` | `Forget` | yes | every durable subscriber (with or without an `Ack`) |

"Durable" means the request carries an `Ack`. Only such a request is held back by a park: it is refused while its aggregate is parked, dropped from the queue when a park is imposed, may be dead-lettered, and parks its aggregate when its attempts run out or it is interrupted. A `DispatchEvent` or an ordinary `DispatchRedelivery` with a nil `Ack` is live only (a redelivery of an ordinary event with a nil `Ack` runs no handler at all). A tombstone, or a redelivery named `ForgetEventName`, with a nil `Ack` is still legal: every durable subscriber runs, with retry, and no ack is called, but the request is queued and run whatever the park state, is never dead-lettered, and a failure ends its ticket with an error without parking the aggregate. The processor always attaches an `Ack` to the redeliveries it builds from the delivery log and to the tombstone `Forget` writes.

## The contract

An implementation carries these ten rules. They are what `dispatchertest.Run` checks.

1. **Order.** Per aggregate, events are handled in `Enqueue` order, one at a time. Different aggregates are independent. `Enqueue` does not block on handlers and never fails because a queue is full.
2. **Live and durable paths.** `DispatchEvent` and `DispatchTombstone` reach the live subscribers; `DispatchRedelivery` never does. A failing live subscriber (an error from `PublishSync`) does not skip the durable delivery. A tombstone, and a redelivered event named `ForgetEventName`, reach every durable subscriber; any other event reaches the matching ones, when it carries an `Ack`.
3. **Durable delivery.** A durable event runs its durable handlers with retry and backoff until all succeed, the attempt cap is hit, or `Interrupt` is called. Then `Ack` runs, and only then does the ticket's `Final` close without error. The aggregate's later events wait for it.
4. **Giving up.** When the attempts of a request that carries an `Ack` run out, `DeadLetter` may accept the event, which counts as delivered (`Ack` runs and `Final` closes with nil). A `DeadLetter` handler that returns an error, or panics, is a refusal. A tombstone, and a redelivered `ForgetEventName`, is never dead-lettered. Otherwise the aggregate is **parked** with `ErrDeliveryParked` wrapping the last handler error and the attempt count as the cause. It is also parked when `Ack` fails (the ack error is the cause), on `Interrupt` while such a request is retrying (`ErrDispatcherClosed`), and on `Park` (the cause given). A request without an `Ack` whose attempts run out, or that is interrupted, ends with an error but does not park the aggregate.
5. **While parked.** A request carrying an `Ack` is not queued. Its ticket settles with `Unrun() == ErrDeliveryParked` and ends with `ErrDeliveryParked`, and its version is recorded as undelivered. Requests without an `Ack` (live events, and tombstones with a nil `Ack`) are still queued and run.
6. **Resume.** `ResumeUndelivered(agg, generation)` lifts only the park of that generation and returns the recorded undelivered versions in ascending order; a stale generation is a no-op (`lifted == false`). `Park(agg, cause, versions...)` imposes a park with a new generation; on an already parked aggregate it keeps the generation, merges the versions and keeps the first non-nil cause (a non-nil cause replaces a nil one). `Inspect` reports the park; the cause is cleared when the park lifts.
7. **Supersede.** `Supersede` abandons the aggregate's queued and retrying durable deliveries and leaves it unparked with no record. The entries stay in the delivery log for the caller to drop.
8. **Tickets.** `TicketFor` finds a ticket exactly while a delivery is queued, running or retrying. It may briefly return a delivery that has just finished, after its `Final` has closed and before the delivery is forgotten; a caller that waits on the ticket gets its outcome at once. `Unrun` answers for events a park left undelivered, until the park lifts.
9. **Handler detection.** `InHandler(ctx, agg)` is true inside a handler this instance runs for `agg`. The processor uses it to refuse `SendWait` and `Forget` from a handler of the same aggregate.
10. **Shutdown.** `Close` waits for the workers to drain, honors its context, and may be retried. `Interrupt` makes retries give up, parking the aggregate of a request that carries an `Ack`. `Interrupted` is closed once either `Interrupt` or `Close` has been called. `WaitIdle` waits for what was in flight when it was called; events queued while it waits are not waited for and are not refused.

After `Close`, `Enqueue` returns `ErrDispatcherClosed`. After `Interrupt` alone it still accepts requests; their durable retries give up and park.

The delivery guarantee is **at-least-once**. The dead-letter handler and the error handlers (`OnPublishError`, `OnDeliveryError`, `OnPanic`) must never take down a worker: a panic in them is swallowed, and a panic in a subscriber handler is recovered and counts as a failed attempt.

### The park state table

An aggregate's delivery state is a single record: running, or parked with a generation, a cause and the undelivered versions. Every transition below must be atomic with respect to the others. The table mirrors the package documentation of the default implementation (`internal/bus/dispatcher/internal/aggregates`); "durable" means a request that carries an `Ack`, and any other request is never held back by a park. The code's `Lift` is `ResumeUndelivered`, and its `ParkIfCurrent(halt)` is the delivery loop's give-up.

| State | Event | Next state | Effect |
|---|---|---|---|
| Running | `Park(agg, cause, v...)` | Parked(gen+1) | record cause; interrupt a delivery in progress; queued durable jobs leave memory, are recorded as undelivered and complete with `ErrDeliveryParked`; live-only jobs stay queued |
| Parked(g) | `Park(...)` | Parked(g) | merge versions; keep the first non-nil cause |
| Parked(g) | `ResumeUndelivered(agg, g)` | Running | return and clear the undelivered versions; clear the cause |
| Parked(g) | `ResumeUndelivered(agg, other)` | Parked(g) | no-op, `lifted == false` |
| Parked(g) | `Enqueue` of a durable request | Parked(g) | not queued; recorded as undelivered; ticket unrun and ends with `ErrDeliveryParked` |
| Running or Parked | `Supersede` | Running | drop queued durable jobs; clear the record |
| Running | the delivery loop gives up and the delivery it started under is still current | Parked(gen+1) | as `Park`, with the give-up cause |
| any | the delivery loop gives up after someone else already settled the aggregate (a `Supersede`, a park, a resume) | unchanged | nothing is parked |
| any | `Inspect` | unchanged | read parked, generation, cause and undelivered; the zero status unless parked |

Generations come from one counter shared by every aggregate, so a park that is lifted and imposed again never repeats a generation a caller has listed. A give-up path (attempts exhausted, ack failure, interrupt) parks first and reports the error second, because the answers a retrying caller gets depend on that order. The interrupt path parks and reports nothing to the delivery error handler.

## Inspect

`Inspect(agg)` returns a read-only `DeliveryStatus`:

| Field | Meaning |
|---|---|
| `Parked` | the aggregate is parked |
| `Generation` | changes each time a park is imposed; pass it to `ResumeUndelivered` |
| `Cause` | the error that parked it; nil when not parked |
| `Undelivered` | ascending versions no handler took while it was parked |

An aggregate that is not parked, or that the dispatcher knows nothing about, returns the zero status. The cause is one of: an error matching `ErrDeliveryParked` that wraps the last handler error and the attempt count, the error from a failed `Ack`, `ErrDispatcherClosed` (interrupted), or the error passed to `Park` (the processor passes the error of a failed `Forget`, enqueue or redelivery). A `nil` cause passed to `Park` is allowed; parking an already parked aggregate keeps the first non-nil cause, so a later non-nil cause replaces a nil one.

The library adds no logger. It reports through the handlers you register, and `Inspect` is how to ask afterwards why an aggregate is parked. From the public API use `Asynx[T].InspectDelivery(aggregateID)` (see [public-api.md](./public-api.md)); `PausedAggregates()` lists the parked aggregates, sorted.

## Plugging one in

```go
ax, err := asynx.New[Order]().
    WithEventStore(store).
    WithSnapshotStore(snapshots).
    WithDispatcher(func(cfg models.DispatcherConfig[Order]) (models.Dispatcher[Order], error) {
        return myDispatcher(cfg)
    }).
    Build()
```

When `WithDispatcher` is not called the in-memory default is used. If the factory returns an error (wrapped as `asynx: dispatcher factory: ...`, so `errors.Is` still sees it), or returns no dispatcher, `Build` fails and nothing is left running (the bus the builder created, or one passed to `WithBus`, is closed). A nil factory passed to `WithDispatcher` is ignored. The factory is called once, after the builder has collected the subscribers, handlers and retry settings, and receives them in `DispatcherConfig`.

### What an implementation is free to do

- Decide where handlers run: in-process goroutines, or a worker reached through a queue.
- Decide how events travel and persist while in flight, and how attempts are counted across restarts. The store's delivery log remains the source of truth for what is unacknowledged; an implementation acknowledges only by calling the `Ack` it was given.
- Choose its own concurrency model, idle handling and resource limits.

What it must not do is change the observable behavior in the ten rules, or write to the delivery log itself.

## Conformance suite

`dispatchertest.Run(t, factory)`, in `github.com/char2cs/asynx/tests/dispatchertest`, checks the contract against any implementation. It is a test kit under `tests/`, not part of the library's root API. It covers ordering, durable retry and ack, dead-letter, park/resume/supersede, the causes and undelivered versions `Inspect` reports, tickets, `Unrun` answers, interrupt and close, `InHandler`, and panics in handlers. The suite runs on its own aggregate type, so the factory is a `models.DispatcherFactory[dispatchertest.Aggregate]`, and it must return a fresh, independent dispatcher on every call. Run it from your implementation's tests:

```go
func TestConformance(t *testing.T) {
    dispatchertest.Run(t, func(cfg models.DispatcherConfig[dispatchertest.Aggregate]) (models.Dispatcher[dispatchertest.Aggregate], error) {
        return newMyDispatcher(cfg), nil
    })
}
```

It does not test goroutine retirement or idle timeouts: those belong to the default implementation's own tests.

## The in-memory default

The default keeps everything in memory in the process.

- One goroutine per active aggregate drains a FIFO queue and retires after an idle timeout. The queue is not bounded.
- Retry: a failed durable delivery retries only the handlers that failed, with exponential backoff from `RetryInitial` doubling up to `RetryMax`. With the defaults that is 100 ms doubling to 30 s, and at most 20 attempts (about six minutes). Each wait is randomised within its upper half (a delay `d` waits between `d/2` and `d`), so deliveries that failed together do not retry in lockstep. Delivery errors are `*models.SubscriberError` values naming the failing subscriber around the handler's error.
- `WithDeliveryRetry` and `WithDeliveryMaxAttempts` set those values; `WithDeadLetterHandler` sets `DeadLetter`.
- The park state is held in memory only. A restart loses it; the delivery log still holds the unacknowledged entries, and the `Build()` recovery redelivers them as `DispatchRedelivery` requests.
- `ResumeDeliveries` lists the parks with their generations (`Parks`), reads the delivery log once for all parked aggregates, then handles one aggregate at a time under that aggregate's lock alone: `ResumeUndelivered(agg, generation)` lifts the park (a stale generation is skipped) and the pending entries plus the returned undelivered versions are re-enqueued in order. If re-enqueueing fails, the aggregate is parked again remembering which versions no handler took, and the errors are joined.

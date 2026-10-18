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
    Ack   func(context.Context) error // nil: live only. non-nil: durable; called once every durable handler succeeded
    Kind  DispatchKind
}

type DispatchKind int // DispatchEvent, DispatchRedelivery, DispatchTombstone

type DeliveryStatus struct {
    Parked      bool
    Generation  uint64  // changes each time a park is imposed
    Cause       error   // why it was parked; nil when not parked
    Undelivered []int64 // ascending versions no handler took
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

`DispatcherConfig` carries **final values**. The builder fills the defaults before calling the factory (20 attempts, 100 ms doubling to 30 s, or whatever `WithDeliveryMaxAttempts` and `WithDeliveryRetry` set), so an implementation never needs to know them. The idle timeout and worker goroutine counts of the default are not part of the config: they are the default's own business.

### Kinds of request

| Kind | Sent by | Live subscribers (`Bus.PublishSync`) | Durable subscribers |
|---|---|---|---|
| `DispatchEvent` | `Send`, `SendWait` | yes | the matching ones, if `Ack != nil` |
| `DispatchRedelivery` | `Build()` recovery, `ResumeDeliveries` | never | the matching ones; every one if the event is named `models.ForgetEventName` |
| `DispatchTombstone` | `Forget` | yes | every durable subscriber |

A request is durable when `Ack != nil`, or when it is a tombstone, or a redelivery named `ForgetEventName`. A tombstone or redelivery with a nil `Ack` is legal: the durable handlers run and no ack is called. A `DispatchEvent` with a nil `Ack` is live only.

## The contract

An implementation carries these ten rules. They are what `dispatchertest.Run` checks.

1. **Order.** Per aggregate, events are handled in `Enqueue` order, one at a time. Different aggregates are independent. `Enqueue` does not block on handlers and never fails because a queue is full.
2. **Live and durable paths.** `DispatchEvent` and `DispatchTombstone` reach the live subscribers; `DispatchRedelivery` never does. A failing live subscriber (an error from `PublishSync`) does not skip the durable delivery. A tombstone, and a redelivered event named `ForgetEventName`, reach every durable subscriber; any other event reaches the matching ones.
3. **Durable delivery.** A durable event runs its durable handlers with retry and backoff until all succeed, the attempt cap is hit, or `Interrupt` is called. Then `Ack` runs, and only then does the ticket's `Final` close without error. The aggregate's later events wait for it.
4. **Giving up.** When attempts run out, `DeadLetter` may accept the event, which counts as delivered (`Ack` runs and `Final` closes with nil). Otherwise the aggregate is **parked** with the cause. It is also parked when `Ack` fails, on `Interrupt`, and on `Park`.
5. **While parked.** A durable event is not queued. Its ticket settles with `Unrun() == ErrDeliveryParked` and ends with `ErrDeliveryParked`, and its version is recorded as undelivered. Live events still flow.
6. **Resume.** `ResumeUndelivered(agg, generation)` lifts only the park of that generation and returns the recorded undelivered versions; a stale generation is a no-op (`lifted == false`). `Park(agg, cause, versions...)` imposes a park with a new generation; on an already parked aggregate it merges the versions and keeps the first cause.
7. **Supersede.** `Supersede` abandons the aggregate's queued and retrying durable deliveries and leaves it unparked with no record.
8. **Tickets.** `TicketFor` returns a ticket exactly while a delivery is queued, running or retrying. `Unrun` answers for events a park left undelivered, until the park lifts.
9. **Handler detection.** `InHandler(ctx, agg)` is true inside a handler this instance runs for `agg`. The processor uses it to refuse `SendWait` and `Forget` from a handler of the same aggregate.
10. **Shutdown.** `Close` waits for the workers to drain, honors its context, and may be retried. `Interrupt` makes retries give up and park. `Interrupted` is closed once either `Interrupt` or `Close` has been called. `WaitIdle` waits for what was in flight when it was called.

After `Close`, `Enqueue` returns `ErrDispatcherClosed`. After `Interrupt` alone it still accepts requests; durable retries give up and park.

The delivery guarantee is **at-least-once**. The dead-letter handler and the error handlers (`OnPublishError`, `OnDeliveryError`, `OnPanic`) must never take down a worker: a panic in them is swallowed, and a panic in a subscriber handler is recovered and counts as a failed attempt.

### The park state table

An aggregate's delivery state is a single record: running, or parked with a generation, a cause and the undelivered versions. Every transition below must be atomic with respect to the others.

| State | Event | Next state | Effect |
|---|---|---|---|
| Running | `Park(agg, cause, v...)` | Parked(gen+1) | queued durable jobs leave memory, are recorded as undelivered and complete with `ErrDeliveryParked` |
| Parked(g) | `Park(...)` | Parked(g) | merge versions; keep the first cause |
| Parked(g) | `ResumeUndelivered(agg, g)` | Running | return and clear the undelivered versions |
| Parked(g) | `ResumeUndelivered(agg, other)` | Parked(g) | no-op, `lifted == false` |
| Parked(g) | `Enqueue` of a durable event | Parked(g) | not queued; recorded as undelivered; ticket ends with `ErrDeliveryParked` |
| Running or Parked | `Supersede` | Running | drop queued durable jobs; clear the record |
| Running | the delivery loop gives up while the park state it started under is still current | Parked(gen+1) | as `Park` |
| any | the delivery loop gives up after someone else already settled the aggregate (a `Supersede`, a resume) | unchanged | nothing is parked |

A give-up path (attempts exhausted, ack failure, interrupt) parks first and reports the error second, because the answers a retrying caller gets depend on that order.

## Inspect

`Inspect(agg)` returns a read-only `DeliveryStatus`:

| Field | Meaning |
|---|---|
| `Parked` | the aggregate is parked |
| `Generation` | changes each time a park is imposed; pass it to `ResumeUndelivered` |
| `Cause` | the error that parked it; nil when not parked |
| `Undelivered` | ascending versions no handler took while it was parked |

An aggregate the dispatcher knows nothing about returns the zero status. In the default implementation the cause is one of: an error matching `ErrDeliveryParked` that wraps the last handler error and the attempt count, the error from a failed `Ack`, `ErrDispatcherClosed` (interrupted), or the error passed to `Park`. A `nil` cause passed to `Park` is allowed; it is replaced by the first non-nil cause of a later park.

The library adds no logger. It reports through the handlers you register, and `Inspect` is how to ask afterwards why an aggregate is parked. From the public API use `Asynx[T].InspectDelivery(aggregateID)` (see [public-api.md](./public-api.md)); `PausedAggregates()` lists the parked aggregates.

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

When `WithDispatcher` is not called the in-memory default is used. If the factory returns an error, `Build` fails with it and the instance built so far is shut down, as for any failed build. The factory is called once, after the builder has collected the subscribers, handlers and retry settings, and receives them in `DispatcherConfig`.

### What an implementation is free to do

- Decide where handlers run: in-process goroutines, or a worker reached through a queue.
- Decide how events travel and persist while in flight, and how attempts are counted across restarts. The store's delivery log remains the source of truth for what is unacknowledged; an implementation acknowledges only by calling the `Ack` it was given.
- Choose its own concurrency model, idle handling and resource limits.

What it must not do is change the observable behavior in the ten rules, or write to the delivery log itself.

## Conformance suite

`dispatchertest.Run(t, factory)` is a public package that checks the contract against any implementation, as `store/storetest.Run` does for `Store`. It covers ordering, durable retry and ack, dead-letter, park/resume/supersede, tickets, `Unrun` answers, interrupt and close, `InHandler`, and panics in handlers. Run it from your implementation's tests:

```go
func TestConformance(t *testing.T) {
    dispatchertest.Run(t, func(cfg models.DispatcherConfig[mocks.Order]) (models.Dispatcher[mocks.Order], error) {
        return newMyDispatcher(cfg)
    })
}
```

It does not test goroutine retirement or idle timeouts: those belong to the default implementation's own tests.

## The in-memory default

The default keeps everything in memory in the process.

- One goroutine per active aggregate drains a FIFO queue and retires after an idle timeout. The queue is not bounded.
- Retry: a failed durable delivery retries only the handlers that failed, with exponential backoff from `RetryInitial` doubling up to `RetryMax`. With the builder defaults that is 100 ms doubling to 30 s, and at most 20 attempts (about six minutes). Each wait is randomised within its upper half (a delay `d` waits between `d/2` and `d`), so deliveries that failed together do not retry in lockstep. Delivery errors are `*models.SubscriberError` values naming the failing subscriber around the handler's error.
- `WithDeliveryRetry` and `WithDeliveryMaxAttempts` set those values; `WithDeadLetterHandler` sets `DeadLetter`.
- The park state is held in memory only. A restart loses it; the delivery log still holds the unacknowledged entries, and the `Build()` recovery redelivers them as `DispatchRedelivery` requests.
- `ResumeDeliveries` lists the parks with their generations (`Parks`), reads the delivery log once for all parked aggregates, then handles one aggregate at a time under that aggregate's lock alone: `ResumeUndelivered(agg, generation)` lifts the park (a stale generation is skipped) and the pending entries plus the returned undelivered versions are re-enqueued in order. If re-enqueueing fails, the aggregate is parked again remembering which versions no handler took, and the errors are joined.

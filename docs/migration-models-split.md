# Migration: the `models` package is split

The single `github.com/char2cs/asynx/models` package (49 files) is gone. Its contents now live in nine focused packages under `models/`. This is a deliberate breaking change to the import path: the module path (`github.com/char2cs/asynx`) is unchanged, and so are the root `asynx`, `store` and `store/storetest` packages; only the types they mention moved.

There is no compatibility shim. Replace the old import with the packages you use and requalify each name using the table below.

## Packages

| Package | Holds |
|---------|-------|
| `models/event` | the events and their delivery records |
| `models/handler` | the callback types |
| `models/subscription` | subscription configuration and options |
| `models/command` | the command contract |
| `models/storage` | the persistence contracts |
| `models/pubsub` | the live bus |
| `models/dispatch` | the request side of delivery |
| `models/dispatcher` | the dispatcher itself |
| `models/failure` | sentinel errors and the subscriber error |

## Old name to new name

| Old | New |
|-----|-----|
| `models.Event` | `event.Event` |
| `models.Delivery` | `event.Delivery` |
| `models.Receipt` | `event.Receipt` |
| `models.Upcaster` | `event.Upcaster` |
| `models.ForgetEventName` | `event.ForgetEventName` |
| `models.ProjectionHandler` | `handler.Projection` |
| `models.PanicHandler` | `handler.Panic` |
| `models.PublishErrorHandler` | `handler.PublishError` |
| `models.DeliveryErrorHandler` | `handler.DeliveryError` |
| `models.DeadLetterHandler` | `handler.DeadLetter` |
| `models.DurableHandler` | `handler.Durable` |
| `models.ForgetHandler` | `handler.Forget` |
| `models.TimeoutHandler` | `handler.Timeout` |
| `models.SubscriptionConfig` | `subscription.Config` |
| `models.SubscriptionOpt` | `subscription.Opt` |
| `models.DurableSub` | `subscription.Durable` |
| `models.WithFallback` | `subscription.WithFallback` |
| `models.WithHandlerTimeout` | `subscription.WithHandlerTimeout` |
| `models.Command` | `command.Command` |
| `models.CommandMeta` | `command.Meta` |
| `models.CommandBehavior` | `command.Behavior` |
| `models.IdempotentCommand` | `command.Idempotent` |
| `models.Store` | `storage.Store` |
| `models.StoreReader` | `storage.StoreReader` |
| `models.DeliveryLog` | `storage.DeliveryLog` |
| `models.SnapshotStore` | `storage.SnapshotStore` |
| `models.Tx` | `storage.Tx` |
| `models.TxDeliveryWriter` | `storage.TxDeliveryWriter` |
| `models.TxNative` | `storage.TxNative` |
| `models.TxStreamWriter` | `storage.TxStreamWriter` |
| `models.Bus` | `pubsub.Bus` |
| `models.BusPublisher` | `pubsub.Publisher` |
| `models.BusSubscriber` | `pubsub.Subscriber` |
| `models.BusLifecycle` | `pubsub.Lifecycle` |
| `models.DispatchKind` | `dispatch.Kind` |
| `models.DispatchEvent` | `dispatch.KindEvent` |
| `models.DispatchRedelivery` | `dispatch.KindRedelivery` |
| `models.DispatchTombstone` | `dispatch.KindTombstone` |
| `models.DispatchRequest` | `dispatch.Request` |
| `models.Ticket` | `dispatch.Ticket` |
| `models.DispatcherQueue` | `dispatch.Queue` |
| `models.DispatcherInspector` | `dispatch.Inspector` |
| `models.DispatcherDelivery` | `dispatch.Intake` |
| `models.DeliveryStatus` | `dispatch.Status` |
| `models.Dispatcher` | `dispatcher.Dispatcher` |
| `models.DispatcherConfig` | `dispatcher.Config` |
| `models.DispatcherFactory` | `dispatcher.Factory` |
| `models.DispatcherLifecycle` | `dispatcher.Lifecycle` |
| `models.DispatcherInterrupt` | `dispatcher.Interrupt` |
| `models.DispatcherShutdown` | `dispatcher.Shutdown` |
| `models.DispatcherParking` | `dispatcher.Parking` |
| `models.SubscriberError` | `failure.SubscriberError` |
| `models.Err*`  (all 23 sentinels, for example `ErrValidation`) | `failure.Err*` (same names) |

## Example

```go
// before
import "github.com/char2cs/asynx/models"

func onOrder(ctx context.Context, event models.Event[Order]) {}

var _ models.ProjectionHandler[Order] = onOrder
if errors.Is(err, models.ErrValidation) {}
```

```go
// after
import (
	"github.com/char2cs/asynx/models/event"
	"github.com/char2cs/asynx/models/failure"
	"github.com/char2cs/asynx/models/handler"
)

func onOrder(ctx context.Context, evt event.Event[Order]) {}

var _ handler.Projection[Order] = onOrder
if errors.Is(err, failure.ErrValidation) {}
```

A local variable or parameter named like one of the new packages (`event`, `handler`, `command`, `dispatcher`) shadows the import in its scope, so rename the variable (`evt`, `callback`, `cmd`) rather than the import.

## Dependencies between the packages

`event`, `failure` and `command` import nothing from `models/`. `handler` imports `event`; `subscription` imports `handler`; `storage` imports `event`; `pubsub` imports `event`, `handler` and `subscription`; `dispatch` imports `event`; `dispatcher` imports `dispatch`, `event`, `handler`, `subscription` and `pubsub`.

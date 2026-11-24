package dispatcher

import (
	"github.com/char2cs/asynx/v2/internal/processor/internal/dispatcher/internal/inmemory"
	"github.com/char2cs/asynx/v2/models/dispatcher"
)

// Dispatcher creates the in-memory Dispatcher from cfg, whose values it takes
// as final: the caller applies the retry defaults. Durable deliveries run the
// subscribers in cfg.DurableSubs on a goroutine per event that takes its
// aggregate's turn, so events of one aggregate are delivered one at a time in
// Enqueue order and different aggregates are independent; live events are
// published through cfg.Bus (nil publishes nothing). Failures and panics are
// reported through cfg.OnPublishError, cfg.OnDeliveryError and cfg.OnPanic,
// each of which may be nil. It implements the whole contract documented on
// models.Dispatcher, and the caller closes it with Close.
func Dispatcher[T any](
	cfg dispatcher.Config[T],
) dispatcher.Dispatcher[T] {
	return inmemory.New(cfg)
}

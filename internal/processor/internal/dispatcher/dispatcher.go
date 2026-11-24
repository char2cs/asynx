package dispatcher

import (
	"github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/inmemory"
	asynxmd "github.com/char2cs/asynx/models"
)

// Dispatcher creates the in-memory Dispatcher from cfg, whose values it takes
// as final: the caller applies the retry defaults. Durable deliveries run the
// subscribers in cfg.DurableSubs on a goroutine per event that takes its aggregate's turn; live
// events are published through cfg.Bus (nil publishes nothing). It implements
// the whole contract documented on models.Dispatcher.
func Dispatcher[T any](
	cfg asynxmd.DispatcherConfig[T],
) asynxmd.Dispatcher[T] {
	return inmemory.New(cfg)
}

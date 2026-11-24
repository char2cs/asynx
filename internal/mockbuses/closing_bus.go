package mockbuses

import (
	"context"
	"sync/atomic"
)

// ClosingBus is a Bus whose Close counts the calls it receives.
type ClosingBus[T any] struct {
	Bus[T]
	Closes atomic.Int32
}

// Close records the call and succeeds.
func (b *ClosingBus[T]) Close(
	_ context.Context,
) error {
	b.Closes.Add(1)
	return nil
}

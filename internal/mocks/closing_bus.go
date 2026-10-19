package mocks

import (
	"context"
	"sync/atomic"
)

// ClosingBus is a Bus whose Close counts the calls it receives.
type ClosingBus struct {
	Bus[Order]
	Closes atomic.Int32
}

// Close records the call and succeeds.
func (b *ClosingBus) Close(
	_ context.Context,
) error {
	b.Closes.Add(1)
	return nil
}

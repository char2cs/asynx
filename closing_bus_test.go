package asynx_test

import (
	"context"
	"sync/atomic"

	"github.com/char2cs/asynx/internal/mocks"
)

type closingBus struct {
	mocks.Bus[mocks.Order]
	closed atomic.Int32
}

func (b *closingBus) Close(
	_ context.Context,
) error {
	b.closed.Add(1)
	return nil
}

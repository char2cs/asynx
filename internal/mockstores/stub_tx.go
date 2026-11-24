package mockstores

import (
	"context"
	"github.com/char2cs/asynx/models/event"
)

type stubTx struct {
	appendFn func(context.Context, string, int64, []byte) error
	deleteFn func(context.Context, string) error
}

// Append discards the data and succeeds.
func (t stubTx) Append(
	ctx context.Context,
	id string,
	v int64,
	d []byte,
) error {
	return t.appendFn(
		ctx,
		id,
		v,
		d,
	)
}

// DeleteStream delegates to the configured delete function.
func (t stubTx) DeleteStream(
	ctx context.Context,
	id string,
) error {
	return t.deleteFn(
		ctx,
		id,
	)
}

// DropDeliveries does nothing and succeeds.
func (stubTx) DropDeliveries(
	context.Context,
	string,
) error {
	return nil
}

// PutReceipt does nothing and succeeds.
func (stubTx) PutReceipt(
	context.Context,
	string,
	event.Receipt,
) error {
	return nil
}

// EnqueueDelivery does nothing and succeeds.
func (stubTx) EnqueueDelivery(
	context.Context,
	string,
	int64,
) error {
	return nil
}

// Native returns nil because the transaction has no native handle.
func (stubTx) Native() any { return nil }

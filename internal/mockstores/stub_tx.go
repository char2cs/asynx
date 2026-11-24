package mockstores

import (
	"context"
	"github.com/char2cs/asynx/models"
)

type stubTx struct {
	appendFn func(context.Context, string, int64, []byte) error
	deleteFn func(context.Context, string) error
}

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

func (t stubTx) DeleteStream(
	ctx context.Context,
	id string,
) error {
	return t.deleteFn(
		ctx,
		id,
	)
}

func (stubTx) DropDeliveries(
	context.Context,
	string,
) error {
	return nil
}

func (stubTx) PutReceipt(
	context.Context,
	string,
	models.Receipt,
) error {
	return nil
}

func (stubTx) EnqueueDelivery(
	context.Context,
	string,
	int64,
) error {
	return nil
}

func (stubTx) Native() any { return nil }

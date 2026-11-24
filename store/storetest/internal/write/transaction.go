package write

import (
	"context"
	"github.com/char2cs/asynx/models/storage"
)

// Transaction returns the function to pass to Store.Atomically that runs ops
// in order under a background context. It returns the error of the first op
// that fails, without running the ones after it, so the store rolls the
// transaction back; with no ops it commits an empty transaction.
func Transaction(
	ops ...Op,
) func(storage.Tx) error {
	return func(
		tx storage.Tx,
	) error {
		return apply(
			tx,
			ops,
		)
	}
}

func apply(
	tx storage.Tx,
	ops []Op,
) (err error) {
	for i := 0; i < len(ops) && err == nil; i++ {
		err = ops[i](
			context.Background(),
			tx,
		)
	}

	return err
}

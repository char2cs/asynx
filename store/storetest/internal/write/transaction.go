package write

import (
	"context"
	"github.com/char2cs/asynx/models"
)

// Transaction returns the function to pass to Store.Atomically that runs ops
// in order under a background context. It returns the error of the first op
// that fails, without running the ones after it, so the store rolls the
// transaction back; with no ops it commits an empty transaction.
func Transaction(
	ops ...Op,
) func(models.Tx) error {
	return func(
		tx models.Tx,
	) error {
		return apply(
			tx,
			ops,
		)
	}
}

func apply(
	tx models.Tx,
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

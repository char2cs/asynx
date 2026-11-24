package write

import (
	"context"
	"github.com/char2cs/asynx/models"
)

// Op is one write inside a transaction: it receives the transaction's Tx and
// the context to write under, and returns the error of its write. Ops are
// composed by Transaction, which stops at the first one that fails.
type Op func(
	ctx context.Context,
	tx models.Tx,
) error

// Abort returns an Op that writes nothing and fails with err, to make a
// transaction roll back after the writes before it.
func Abort(
	err error,
) Op {
	return func(
		context.Context,
		models.Tx,
	) error {
		return err
	}
}

// Native returns an Op that asks the transaction for its native handle and
// discards it, to check that the call succeeds inside a transaction. The
// handle itself is store specific, and nil for stores that have none, so
// nothing can be asserted about it.
func Native() Op {
	return func(
		_ context.Context,
		tx models.Tx,
	) error {
		tx.Native()
		return nil
	}
}

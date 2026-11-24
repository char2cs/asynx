package write

import (
	"context"
	"github.com/char2cs/asynx/models"
)

// Receipt returns an Op that stores r as the receipt of commandID.
func Receipt(
	commandID string,
	r models.Receipt,
) Op {
	return func(
		ctx context.Context,
		tx models.Tx,
	) error {
		return tx.PutReceipt(
			ctx,
			commandID,
			r,
		)
	}
}

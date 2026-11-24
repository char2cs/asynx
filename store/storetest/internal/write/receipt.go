package write

import (
	"context"
	"github.com/char2cs/asynx/v2/models/event"
	"github.com/char2cs/asynx/v2/models/storage"
)

// Receipt returns an Op that stores r as the receipt of commandID.
func Receipt(
	commandID string,
	r event.Receipt,
) Op {
	return func(
		ctx context.Context,
		tx storage.Tx,
	) error {
		return tx.PutReceipt(
			ctx,
			commandID,
			r,
		)
	}
}

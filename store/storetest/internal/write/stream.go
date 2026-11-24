package write

import (
	"context"
	"github.com/char2cs/asynx/models/storage"
)

// Blob returns an Op that appends data to streamID at version.
func Blob(
	streamID string,
	version int64,
	data []byte,
) Op {
	return func(
		ctx context.Context,
		tx storage.Tx,
	) error {
		return tx.Append(
			ctx,
			streamID,
			version,
			data,
		)
	}
}

// Blobs returns one Op per blob that appends the blobs to streamID at versions
// 1, 2, 3 and so on, in the order given.
func Blobs(
	streamID string,
	blobs ...string,
) []Op {
	ops := make(
		[]Op,
		len(blobs),
	)
	for i, blob := range blobs {
		ops[i] = Blob(
			streamID,
			int64(i+1),
			[]byte(blob),
		)
	}

	return ops
}

// EraseStream returns an Op that deletes every record of streamID.
func EraseStream(
	streamID string,
) Op {
	return func(
		ctx context.Context,
		tx storage.Tx,
	) error {
		return tx.DeleteStream(
			ctx,
			streamID,
		)
	}
}

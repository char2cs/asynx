package store

import (
	"context"
	"fmt"
	"github.com/char2cs/asynx/v2/models/event"
	"github.com/char2cs/asynx/v2/models/failure"
	"slices"
)

type memoryTx struct {
	s    *memoryStore
	undo []func()

	committed bool
}

func (t *memoryTx) commit() {
	t.committed = true
}

func (t *memoryTx) close() {
	defer t.s.txMu.Unlock()
	if t.committed {
		return
	}

	t.rollback()
}

func (t *memoryTx) onRollback(
	undo func(),
) {
	t.undo = append(
		t.undo,
		undo,
	)
}

func (t *memoryTx) rollback() {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	for i := len(t.undo) - 1; i >= 0; i-- {
		t.undo[i]()
	}

	t.undo = nil
}

// Native returns nil: the in-memory store has no underlying transaction
// handle, so synchronous projections have nothing to write through.
func (t *memoryTx) Native() any { return nil }

// Append stores data at (streamID, version) and keeps the stream sorted by
// version, so versions may arrive in any order. A version already stored
// returns an error wrapping failure.ErrPipelineFailed; an error scheduled with
// SetError for streamID is returned instead, once, with nothing stored. The
// write is undone if the transaction rolls back.
func (t *memoryTx) Append(
	ctx context.Context,
	streamID string,
	version int64,
	data []byte,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s := t.s
	s.mu.Lock()
	defer s.mu.Unlock()

	if err, ok := s.errOn[streamID]; ok {
		delete(
			s.errOn,
			streamID,
		)
		return err
	}

	return t.insert(
		streamID,
		version,
		data,
	)
}

func (t *memoryTx) insert(
	streamID string,
	version int64,
	data []byte,
) error {
	s := t.s
	entries := s.streams[streamID]
	idx, found := slices.BinarySearchFunc(
		entries,
		version,
		compareEntryVersion,
	)
	if found {
		return fmt.Errorf(
			"%w: version conflict (%s, v%d)",
			failure.ErrPipelineFailed,
			streamID,
			version,
		)
	}

	s.streams[streamID] = slices.Insert(
		entries,
		idx,
		entry{version: version, data: data},
	)
	t.onRollback(func() {
		s.streams[streamID] = slices.Delete(
			s.streams[streamID],
			idx,
			idx+1,
		)
	})
	return nil
}

// DeleteStream removes every record of streamID; an unknown stream is not an
// error. A rollback restores the stream as it was.
func (t *memoryTx) DeleteStream(
	ctx context.Context,
	streamID string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s := t.s
	s.mu.Lock()
	defer s.mu.Unlock()

	old, existed := s.streams[streamID]
	delete(
		s.streams,
		streamID,
	)
	if existed {
		t.onRollback(func() { s.streams[streamID] = old })
	}

	return nil
}

// DropDeliveries removes every pending delivery of aggregateID and keeps the
// order of the others. A rollback restores the entries where they were.
func (t *memoryTx) DropDeliveries(
	ctx context.Context,
	aggregateID string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s := t.s
	s.mu.Lock()
	defer s.mu.Unlock()

	old := s.deliveries
	s.deliveries = slices.DeleteFunc(
		slices.Clone(old),
		aggregateDeliveries(aggregateID).belong,
	)
	t.onRollback(func() { s.deliveries = old })
	return nil
}

// PutReceipt stores the receipt for commandID. A receipt that already exists,
// from this or an earlier transaction, returns an error wrapping
// failure.ErrDuplicateReceipt and is left unchanged.
func (t *memoryTx) PutReceipt(
	ctx context.Context,
	commandID string,
	r event.Receipt,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s := t.s
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.receipts[commandID]; exists {
		return fmt.Errorf(
			"%w: %s",
			failure.ErrDuplicateReceipt,
			commandID,
		)
	}

	s.receipts[commandID] = r
	t.onRollback(func() {
		delete(
			s.receipts,
			commandID,
		)
	})
	return nil
}

// EnqueueDelivery appends (aggregateID, version) to the delivery log, after
// every entry already in it. The entry is removed if the transaction rolls
// back.
func (t *memoryTx) EnqueueDelivery(
	ctx context.Context,
	aggregateID string,
	version int64,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s := t.s
	s.mu.Lock()
	defer s.mu.Unlock()

	old := s.deliveries
	s.deliveries = append(
		slices.Clone(old),
		event.Delivery{AggregateID: aggregateID, Version: version},
	)
	t.onRollback(func() { s.deliveries = old })
	return nil
}

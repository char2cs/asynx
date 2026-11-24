package store

import (
	"context"
	"fmt"
	"github.com/char2cs/asynx/models"
	"slices"
)

type memoryTx struct {
	s    *memoryStore
	undo []func()
}

func (t *memoryTx) rollbackUnless(
	committed *bool,
) {
	if *committed {
		return
	}

	t.rollback()
}

func (t *memoryTx) rollback() {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	for i := len(t.undo) - 1; i >= 0; i-- {
		t.undo[i]()
	}

	t.undo = nil
}

// Native returns nil: the in-memory store has no underlying handle.
func (t *memoryTx) Native() any { return nil }

// Append stores data at (streamID, version), failing on a version conflict.
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
			models.ErrPipelineFailed,
			streamID,
			version,
		)
	}

	s.streams[streamID] = slices.Insert(
		entries,
		idx,
		entry{version: version, data: data},
	)
	undo := func() {
		s.removeEntry(
			streamID,
			version,
		)
	}
	t.undo = append(
		t.undo,
		undo,
	)
	return nil
}

// DeleteStream removes every record of streamID.
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
		undo := func() { s.streams[streamID] = old }
		t.undo = append(
			t.undo,
			undo,
		)
	}

	return nil
}

// DropDeliveries removes every pending delivery of aggregateID.
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
	ofAggregate := func(
		d models.Delivery,
	) bool {
		return d.AggregateID == aggregateID
	}
	s.deliveries = slices.DeleteFunc(
		slices.Clone(old),
		ofAggregate,
	)
	undo := func() { s.deliveries = old }
	t.undo = append(
		t.undo,
		undo,
	)
	return nil
}

// PutReceipt stores the receipt for commandID, failing on a duplicate.
func (t *memoryTx) PutReceipt(
	ctx context.Context,
	commandID string,
	r models.Receipt,
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
			models.ErrDuplicateReceipt,
			commandID,
		)
	}

	s.receipts[commandID] = r
	undo := func() {
		delete(
			s.receipts,
			commandID,
		)
	}
	t.undo = append(
		t.undo,
		undo,
	)
	return nil
}

// EnqueueDelivery records that (aggregateID, version) awaits delivery.
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
		models.Delivery{AggregateID: aggregateID, Version: version},
	)
	undo := func() { s.deliveries = old }
	t.undo = append(
		t.undo,
		undo,
	)
	return nil
}

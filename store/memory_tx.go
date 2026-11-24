package store

import (
	"cmp"
	"context"
	"fmt"
	"github.com/char2cs/asynx/models"
	"slices"
)

type memoryTx struct {
	s    *memoryStore
	undo []func()
}

func (t *memoryTx) rollback() {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	for i := len(t.undo) - 1; i >= 0; i-- {
		t.undo[i]()
	}

	t.undo = nil
}

func (t *memoryTx) Native() any { return nil }

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

	entries := s.streams[streamID]
	idx, found := slices.BinarySearchFunc(
		entries,
		version,
		func(
			e entry,
			v int64,
		) int {
			return cmp.Compare(
				e.version,
				v,
			)
		},
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
	t.undo = append(
		t.undo,
		func() {
			s.removeEntry(
				streamID,
				version,
			)
		},
	)
	return nil
}

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
		t.undo = append(
			t.undo,
			func() { s.streams[streamID] = old },
		)
	}

	return nil
}

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
		func(
			d models.Delivery,
		) bool {
			return d.AggregateID == aggregateID
		},
	)
	t.undo = append(
		t.undo,
		func() { s.deliveries = old },
	)
	return nil
}

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
	t.undo = append(
		t.undo,
		func() {
			delete(
				s.receipts,
				commandID,
			)
		},
	)
	return nil
}

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
	t.undo = append(
		t.undo,
		func() { s.deliveries = old },
	)
	return nil
}

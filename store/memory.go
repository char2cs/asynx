// Package store provides an in-memory implementation of models.Store,
// intended for tests and examples. Data is not persisted across restarts;
// production deployments should implement models.Store over a durable backend
// and prove it with store/storetest.
package store

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/char2cs/asynx/models"
)

// Memory is a thread-safe in-memory store for tests and examples.
// It satisfies models.Store, adds failure injection via SetError, and offers
// Append/Delete helpers for seeding streams without writing a transaction.
//
// Transactions are serialized and rolled back through an undo log. Isolation
// is read-uncommitted: reads outside a transaction may observe its writes
// before it commits.
type Memory interface {
	models.Store

	// SetError schedules a one-shot error for the next Append call on
	// streamID. The error is consumed and removed after the first
	// matching Append.
	SetError(
		streamID string,
		err error,
	)

	// Append writes one blob in its own transaction.
	Append(
		ctx context.Context,
		streamID string,
		version int64,
		data []byte,
	) error

	// Delete removes streamID in its own transaction.
	Delete(
		ctx context.Context,
		streamID string,
	) error
}

type memory struct {
	// txMu serializes every mutation (transactions and acks) so an undo log
	// can restore state without clobbering a concurrent writer.
	txMu sync.Mutex

	mu         sync.RWMutex
	streams    map[string][]entry
	receipts   map[string]models.Receipt
	deliveries []models.Delivery
	errOn      map[string]error
}

type entry struct {
	version int64
	data    []byte
}

// New returns an empty in-memory store.
func New() Memory {
	return &memory{
		streams:  make(map[string][]entry),
		receipts: make(map[string]models.Receipt),
		errOn:    make(map[string]error),
	}
}

func (s *memory) SetError(
	streamID string,
	err error,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errOn[streamID] = err
}

func (s *memory) Append(
	ctx context.Context,
	streamID string,
	version int64,
	data []byte,
) error {
	return s.Atomically(ctx, func(tx models.Tx) error {
		return tx.Append(ctx, streamID, version, data)
	})
}

func (s *memory) Delete(
	ctx context.Context,
	streamID string,
) error {
	return s.Atomically(ctx, func(tx models.Tx) error {
		return tx.DeleteStream(ctx, streamID)
	})
}

func (s *memory) Atomically(
	ctx context.Context,
	fn func(tx models.Tx) error,
) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.txMu.Lock()
	defer s.txMu.Unlock()

	tx := &memoryTx{s: s}
	committed := false
	defer func() {
		if !committed {
			tx.rollback()
		}
	}()

	if err := fn(tx); err != nil {
		return err
	}
	committed = true
	return nil
}

func (s *memory) ReadFrom(
	ctx context.Context,
	streamID string,
	fromVersion int64,
) ([][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return toBlobs(s.entriesFrom(streamID, fromVersion)), nil
}

func (s *memory) ReadRange(
	ctx context.Context,
	streamID string,
	fromVersion int64,
	count int64,
) ([][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := s.entriesFrom(streamID, fromVersion)
	if int64(len(entries)) > count {
		entries = entries[:count]
	}

	return toBlobs(entries), nil
}

func (s *memory) GetReceipt(
	ctx context.Context,
	commandID string,
) (models.Receipt, bool, error) {
	if err := ctx.Err(); err != nil {
		return models.Receipt{}, false, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	r, ok := s.receipts[commandID]
	return r, ok, nil
}

func (s *memory) PendingDeliveries(
	ctx context.Context,
	limit int,
) ([]models.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	n := min(limit, len(s.deliveries))
	return slices.Clone(s.deliveries[:n]), nil
}

func (s *memory) AckDelivery(
	ctx context.Context,
	aggregateID string,
	version int64,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.txMu.Lock()
	defer s.txMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()

	target := models.Delivery{AggregateID: aggregateID, Version: version}
	if i := slices.Index(s.deliveries, target); i >= 0 {
		s.deliveries = slices.Delete(s.deliveries, i, i+1)
	}
	return nil
}

func (s *memory) entriesFrom(
	streamID string,
	fromVersion int64,
) []entry {
	entries := s.streams[streamID]
	if len(entries) == 0 {
		return nil
	}
	startIdx, _ := slices.BinarySearchFunc(entries, fromVersion, func(e entry, v int64) int {
		return cmp.Compare(e.version, v)
	})
	return entries[startIdx:]
}

func toBlobs(entries []entry) [][]byte {
	result := make([][]byte, len(entries))
	for i, e := range entries {
		result[i] = e.data
	}
	return result
}

// memoryTx applies writes immediately and records how to undo them.
type memoryTx struct {
	s    *memory
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
		delete(s.errOn, streamID)
		return err
	}

	entries := s.streams[streamID]
	idx, found := slices.BinarySearchFunc(entries, version, func(e entry, v int64) int {
		return cmp.Compare(e.version, v)
	})
	if found {
		return fmt.Errorf("%w: version conflict (%s, v%d)", models.ErrPipelineFailed, streamID, version)
	}

	s.streams[streamID] = slices.Insert(entries, idx, entry{version: version, data: data})
	t.undo = append(t.undo, func() {
		cur := s.streams[streamID]
		if i, ok := slices.BinarySearchFunc(cur, version, func(e entry, v int64) int {
			return cmp.Compare(e.version, v)
		}); ok {
			s.streams[streamID] = slices.Delete(cur, i, i+1)
		}
	})
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
	delete(s.streams, streamID)
	if existed {
		t.undo = append(t.undo, func() { s.streams[streamID] = old })
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
	s.deliveries = slices.DeleteFunc(slices.Clone(old), func(d models.Delivery) bool {
		return d.AggregateID == aggregateID
	})
	t.undo = append(t.undo, func() { s.deliveries = old })
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
		return fmt.Errorf("%w: %s", models.ErrDuplicateReceipt, commandID)
	}
	s.receipts[commandID] = r
	t.undo = append(t.undo, func() { delete(s.receipts, commandID) })
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
	s.deliveries = append(slices.Clone(old), models.Delivery{AggregateID: aggregateID, Version: version})
	t.undo = append(t.undo, func() { s.deliveries = old })
	return nil
}

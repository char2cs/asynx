package store

import (
	"cmp"
	"context"
	"github.com/char2cs/asynx/v2/models/event"
	"github.com/char2cs/asynx/v2/models/storage"
	"slices"
	"sync"
)

type memoryStore struct {
	txMu sync.Mutex

	mu         sync.RWMutex
	streams    map[string][]entry
	receipts   map[string]event.Receipt
	deliveries []event.Delivery
	errOn      map[string]error
}

// New returns an empty in-memory store, ready for concurrent use. Each call
// returns an independent store: nothing is shared between them and nothing
// outlives the process.
func New() Memory {
	return &memoryStore{
		streams:  make(map[string][]entry),
		receipts: make(map[string]event.Receipt),
		errOn:    make(map[string]error),
	}
}

// SetError schedules a one-shot error for the next Append on streamID, made
// either directly or through a transaction. The error is returned by that
// Append unchanged, the write is not performed, and the schedule is then
// cleared. Scheduling again before it fired replaces the earlier error. Other
// streams are not affected.
func (s *memoryStore) SetError(
	streamID string,
	err error,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errOn[streamID] = err
}

// Append writes one blob in its own transaction, so it commits or fails on its
// own: a duplicate (streamID, version) returns an error wrapping
// failure.ErrPipelineFailed and a scheduled SetError error is returned as is.
// It is meant for seeding streams in tests without writing a transaction.
func (s *memoryStore) Append(
	ctx context.Context,
	streamID string,
	version int64,
	data []byte,
) error {
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}

	defer tx.close()

	err = tx.Append(
		ctx,
		streamID,
		version,
		data,
	)
	if err != nil {
		return err
	}

	tx.commit()
	return nil
}

// Delete removes every blob of streamID in its own transaction. Deleting a
// stream that does not exist is not an error, and other streams are untouched.
// It is meant for seeding streams in tests without writing a transaction.
func (s *memoryStore) Delete(
	ctx context.Context,
	streamID string,
) error {
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}

	defer tx.close()

	err = tx.DeleteStream(
		ctx,
		streamID,
	)
	if err != nil {
		return err
	}

	tx.commit()
	return nil
}

// Atomically runs fn in a transaction, committing when fn returns nil and
// rolling every write back when it returns an error or panics (the panic then
// propagates). Transactions are serialized: a second Atomically waits for the
// first to finish, so fn must not call Atomically or any seeding method of
// the same store. Isolation is read-uncommitted: reads outside the transaction
// may observe its writes before it commits, and would then see them vanish on
// rollback. A context that is already done returns its error without running
// fn.
func (s *memoryStore) Atomically(
	ctx context.Context,
	fn func(tx storage.Tx) error,
) error {
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}

	defer tx.close()

	err = fn(tx)
	if err != nil {
		return err
	}

	tx.commit()
	return nil
}

func (s *memoryStore) begin(
	ctx context.Context,
) (*memoryTx, error) {
	err := ctx.Err()
	if err != nil {
		return nil,
			err
	}

	s.txMu.Lock()
	return &memoryTx{s: s},
		nil
}

// ReadFrom returns the blobs of streamID with a version of at least
// fromVersion, in ascending version order whatever order they were appended in.
// An unknown stream yields an empty result. The blobs are the stored slices,
// not copies, so callers must not modify them.
func (s *memoryStore) ReadFrom(
	ctx context.Context,
	streamID string,
	fromVersion int64,
) ([][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil,
			err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return toBlobs(s.entriesFrom(
			streamID,
			fromVersion,
		)),
		nil
}

// ReadRange returns at most count blobs of streamID with a version of at least
// fromVersion, in ascending version order. A count of zero or less returns
// nothing; an unknown stream yields an empty result. The blobs are the stored
// slices, not copies, so callers must not modify them.
func (s *memoryStore) ReadRange(
	ctx context.Context,
	streamID string,
	fromVersion int64,
	count int64,
) ([][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil,
			err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	entries := s.entriesFrom(
		streamID,
		fromVersion,
	)
	entries = entries[:min(
		int64(len(entries)),
		max(
			count,
			0,
		),
	)]

	return toBlobs(entries),
		nil
}

// GetReceipt returns the receipt stored for commandID; found is false when
// there is none. Receipts of an open transaction are visible before it commits
// (read-uncommitted) and gone again if it rolls back.
func (s *memoryStore) GetReceipt(
	ctx context.Context,
	commandID string,
) (event.Receipt, bool, error) {
	if err := ctx.Err(); err != nil {
		return event.Receipt{},
			false,
			err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	r, ok := s.receipts[commandID]
	return r,
		ok,
		nil
}

// PendingDeliveries returns up to limit unacknowledged deliveries in enqueue
// order, as a copy the caller owns. A limit of zero or less returns none.
// Listing does not remove anything.
func (s *memoryStore) PendingDeliveries(
	ctx context.Context,
	limit int,
) ([]event.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil,
			err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	n := max(
		0,
		min(
			limit,
			len(s.deliveries),
		),
	)
	return slices.Clone(s.deliveries[:n]),
		nil
}

// AckDelivery removes the delivery for (aggregateID, version) if it is pending
// and does nothing otherwise, so acknowledging twice, or acknowledging an
// entry a Forget already dropped, is not an error. It waits for an open
// transaction to finish first, so it never acknowledges an entry that the
// transaction is about to roll back.
func (s *memoryStore) AckDelivery(
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

	target := event.Delivery{AggregateID: aggregateID, Version: version}
	if i := slices.Index(
		s.deliveries,
		target,
	); i >= 0 {
		s.deliveries = slices.Delete(
			s.deliveries,
			i,
			i+1,
		)
	}

	return nil
}

func (s *memoryStore) entriesFrom(
	streamID string,
	fromVersion int64,
) []entry {
	entries := s.streams[streamID]
	if len(entries) == 0 {
		return nil
	}

	startIdx, _ := slices.BinarySearchFunc(
		entries,
		fromVersion,
		compareEntryVersion,
	)
	return entries[startIdx:]
}

func compareEntryVersion(
	e entry,
	v int64,
) int {
	return cmp.Compare(
		e.version,
		v,
	)
}

func toBlobs(
	entries []entry,
) [][]byte {
	result := make(
		[][]byte,
		len(entries),
	)
	for i, e := range entries {
		result[i] = e.data
	}

	return result
}

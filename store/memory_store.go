package store

import (
	"cmp"
	"context"
	"github.com/char2cs/asynx/models"
	"slices"
	"sync"
)

type memoryStore struct {
	txMu sync.Mutex

	mu         sync.RWMutex
	streams    map[string][]entry
	receipts   map[string]models.Receipt
	deliveries []models.Delivery
	errOn      map[string]error
}

// New returns an empty in-memory store.
func New() Memory {
	return &memoryStore{
		streams:  make(map[string][]entry),
		receipts: make(map[string]models.Receipt),
		errOn:    make(map[string]error),
	}
}

func (s *memoryStore) SetError(
	streamID string,
	err error,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errOn[streamID] = err
}

func (s *memoryStore) Append(
	ctx context.Context,
	streamID string,
	version int64,
	data []byte,
) error {
	return s.Atomically(
		ctx,
		func(
			tx models.Tx,
		) error {
			return tx.Append(
				ctx,
				streamID,
				version,
				data,
			)
		},
	)
}

func (s *memoryStore) Delete(
	ctx context.Context,
	streamID string,
) error {
	return s.Atomically(
		ctx,
		func(
			tx models.Tx,
		) error {
			return tx.DeleteStream(
				ctx,
				streamID,
			)
		},
	)
}

func (s *memoryStore) Atomically(
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
	if int64(len(entries)) > count {
		entries = entries[:count]
	}

	return toBlobs(entries),
		nil
}

func (s *memoryStore) GetReceipt(
	ctx context.Context,
	commandID string,
) (models.Receipt, bool, error) {
	if err := ctx.Err(); err != nil {
		return models.Receipt{},
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

func (s *memoryStore) PendingDeliveries(
	ctx context.Context,
	limit int,
) ([]models.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil,
			err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	n := min(
		limit,
		len(s.deliveries),
	)
	return slices.Clone(s.deliveries[:n]),
		nil
}

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

	target := models.Delivery{AggregateID: aggregateID, Version: version}
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
	return entries[startIdx:]
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

func (s *memoryStore) removeEntry(
	streamID string,
	version int64,
) {
	cur := s.streams[streamID]
	i, found := slices.BinarySearchFunc(
		cur,
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
	if !found {
		return
	}

	s.streams[streamID] = slices.Delete(
		cur,
		i,
		i+1,
	)
}

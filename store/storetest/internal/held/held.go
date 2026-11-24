package held

import (
	"context"
	"github.com/char2cs/asynx/models/storage"
	"sync"
	"testing"
)

// Transaction is a store transaction that has appended one blob and is kept
// open until Commit is called, so a test can act while a transaction is in
// flight.
type Transaction struct {
	release   chan struct{}
	committed chan error
}

// Begin opens a transaction on s that appends the blob "a" to stream "s1" at
// version 1, and returns once that append has been made, with the transaction
// still open. It fails t if the store refuses to run the transaction. The
// transaction stays open, holding whatever locks the store takes, until
// Commit.
func Begin(
	t testing.TB,
	s storage.Store,
) *Transaction {
	t.Helper()
	h := &Transaction{
		release: make(chan struct{}),
		committed: make(
			chan error,
			1,
		),
	}
	appended := make(chan struct{})
	signal := sync.OnceFunc(func() { close(appended) })
	go h.run(
		s,
		signal,
	)

	<-appended
	return h
}

func (h *Transaction) run(
	s storage.Store,
	signalAppended func(),
) {
	err := s.Atomically(
		context.Background(),
		h.appendThenWait(signalAppended),
	)
	signalAppended()
	h.committed <- err
}

func (h *Transaction) appendThenWait(
	signalAppended func(),
) func(storage.Tx) error {
	return func(
		tx storage.Tx,
	) error {
		err := tx.Append(
			context.Background(),
			"s1",
			1,
			[]byte("a"),
		)
		signalAppended()
		<-h.release
		return err
	}
}

// Commit lets the transaction finish and fails t if the store did not commit
// it. It returns after the commit, so the appended blob is then durable.
func (h *Transaction) Commit(
	t testing.TB,
) {
	t.Helper()
	close(h.release)
	if err := <-h.committed; err != nil {
		t.Fatalf(
			"the open transaction failed to commit: %v",
			err,
		)
	}
}

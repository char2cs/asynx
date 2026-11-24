package held_test

import (
	"context"
	"errors"
	"github.com/char2cs/asynx/v2/models/storage"
	"github.com/char2cs/asynx/v2/store"
	"github.com/char2cs/asynx/v2/store/storetest/internal/held"
	"runtime"
	"sync/atomic"
	"testing"
)

func TestBegin_ReturnsWithTheTransactionStillOpenAndCommitFinishesIt(
	t *testing.T,
) {
	s := store.New()

	tx := held.Begin(
		t,
		s,
	)
	tx.Commit(t)

	blobs, err := s.ReadFrom(
		context.Background(),
		"s1",
		1,
	)
	if err != nil ||
		len(blobs) != 1 ||
		string(blobs[0]) != "a" {
		t.Fatalf(
			"after Commit ReadFrom = %q, %v; want the appended blob",
			blobs,
			err,
		)
	}
}

type refusingStore struct{ storage.Store }

var errRefused = errors.New("refused")

// Atomically refuses to run the function.
func (refusingStore) Atomically(
	context.Context,
	func(storage.Tx) error,
) error {
	return errRefused
}

type failureRecorder struct {
	testing.TB
	failed atomic.Bool
}

// Helper is a no-op so the recorder stays out of the way.
func (r *failureRecorder) Helper() {}

// Fatalf records the failure and stops the calling goroutine.
func (r *failureRecorder) Fatalf(
	string,
	...any,
) {
	r.failed.Store(true)
	runtime.Goexit()
}

func TestCommit_FailsTheTestWhenTheStoreRefusedTheTransaction(
	t *testing.T,
) {
	r := &failureRecorder{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		held.Begin(
			r,
			refusingStore{},
		).Commit(r)
	}()
	<-done

	if !r.failed.Load() {
		t.Fatal("a store that refused the transaction did not fail the test")
	}
}

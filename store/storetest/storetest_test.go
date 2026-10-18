package storetest

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
)

func TestRun_PassesOnTheMemoryStore(t *testing.T) {
	Run(t, func(*testing.T) models.Store { return store.New() })
}

func TestMemorySnapshotStoreConforms(t *testing.T) {
	RunSnapshots(t, func(*testing.T) models.SnapshotStore { return store.NewSnapshots() })
}

// recorder stands in for *testing.T so a scenario can fail without failing the
// test that runs it.
type recorder struct {
	testing.TB
	failed atomic.Bool
}

func (r *recorder) Helper() {}

func (r *recorder) Errorf(
	string,
	...any,
) {
	r.failed.Store(true)
}

func (r *recorder) Error(...any) { r.failed.Store(true) }

func (r *recorder) Fatalf(
	string,
	...any,
) {
	r.failed.Store(true)
	runtime.Goexit()
}

func (r *recorder) Fatal(...any) {
	r.failed.Store(true)
	runtime.Goexit()
}

func failures[S any](
	scenarios map[string]scenario[S],
	subject S,
) map[string]bool {
	var mu sync.Mutex
	failed := map[string]bool{}
	var wg sync.WaitGroup
	for name, check := range scenarios {
		wg.Add(1)
		go func() {
			defer wg.Done()
			didFail := runScenario(check, subject)
			mu.Lock()
			failed[name] = didFail
			mu.Unlock()
		}()
	}
	wg.Wait()
	return failed
}

func runScenario[S any](
	check scenario[S],
	subject S,
) bool {
	r := &recorder{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		check(r, subject)
	}()
	<-done
	return r.failed.Load()
}

func countFailed(failed map[string]bool) int {
	n := 0
	for _, f := range failed {
		if f {
			n++
		}
	}
	return n
}

// hollowStore accepts every write and keeps none, and reads nothing back.
type hollowStore struct{ models.Store }

func (hollowStore) Atomically(
	context.Context,
	func(models.Tx) error,
) error {
	return nil
}

// brokenStore fails every operation.
type brokenStore struct{ models.Store }

var errBroken = errors.New("broken")

func (brokenStore) Atomically(
	context.Context,
	func(models.Tx) error,
) error {
	return errBroken
}

func (brokenStore) ReadFrom(
	context.Context,
	string,
	int64,
) ([][]byte, error) {
	return nil, errBroken
}

func (brokenStore) ReadRange(
	context.Context,
	string,
	int64,
	int64,
) ([][]byte, error) {
	return nil, errBroken
}

func (brokenStore) GetReceipt(
	context.Context,
	string,
) (models.Receipt, bool, error) {
	return models.Receipt{}, false, errBroken
}

func (brokenStore) PendingDeliveries(
	context.Context,
	int,
) ([]models.Delivery, error) {
	return nil, errBroken
}

func (brokenStore) AckDelivery(
	context.Context,
	string,
	int64,
) error {
	return errBroken
}

func TestSuite_FlagsStoresThatDoNotKeepWhatTheyAccepted(t *testing.T) {
	failed := failures(storeScenarios(), models.Store(hollowStore{Store: store.New()}))

	if got, total := countFailed(failed), len(failed); got < total/2 {
		t.Fatalf("only %d of %d scenarios failed a store that keeps nothing", got, total)
	}
	if !failed["AppendCommitsAndReadFromReturnsInOrder"] {
		t.Fatal("a store that keeps nothing passed the append scenario")
	}
}

func TestSuite_FlagsStoresThatFail(t *testing.T) {
	failed := failures(storeScenarios(), models.Store(brokenStore{Store: store.New()}))

	if got, total := countFailed(failed), len(failed); got < total/2 {
		t.Fatalf("only %d of %d scenarios failed a store that errors on everything", got, total)
	}
}

// forgetfulSnapshots drops every snapshot it is given.
type forgetfulSnapshots struct{ models.SnapshotStore }

func (forgetfulSnapshots) Put(
	context.Context,
	string,
	int64,
	[]byte,
) error {
	return nil
}

// failingSnapshots fails every operation.
type failingSnapshots struct{ models.SnapshotStore }

func (failingSnapshots) Put(
	context.Context,
	string,
	int64,
	[]byte,
) error {
	return errBroken
}

func (failingSnapshots) Get(
	context.Context,
	string,
) ([]byte, bool, error) {
	return nil, false, errBroken
}

func (failingSnapshots) Delete(
	context.Context,
	string,
) error {
	return errBroken
}

func TestSnapshotSuite_FlagsStoresThatDoNotKeepWhatTheyAccepted(t *testing.T) {
	failed := failures(snapshotScenarios(), models.SnapshotStore(forgetfulSnapshots{SnapshotStore: store.NewSnapshots()}))

	if !failed["PutThenGetReturnsTheBytes"] || !failed["PutReplacesTheSnapshot"] {
		t.Fatalf("a snapshot store that keeps nothing passed: %v", failed)
	}
}

func TestSnapshotSuite_FlagsStoresThatFail(t *testing.T) {
	failed := failures(snapshotScenarios(), models.SnapshotStore(failingSnapshots{SnapshotStore: store.NewSnapshots()}))

	if got, total := countFailed(failed), len(failed); got < total/2 {
		t.Fatalf("only %d of %d scenarios failed a store that errors on everything", got, total)
	}
}

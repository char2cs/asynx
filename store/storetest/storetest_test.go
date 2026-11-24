package storetest

import (
	"context"
	"errors"
	"github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func TestRun_PassesOnTheMemoryStore(
	t *testing.T,
) {
	newStore := func(
		*testing.T,
	) models.Store {
		return store.New()
	}
	Run(
		t,
		newStore,
	)
}

func runOnMemoryStore(
	t *testing.T,
	scenarios map[string]scenario[models.Store],
) {
	for name, sc := range scenarios {
		t.Run(
			name,
			memoryCase(sc),
		)
	}
}

func memoryCase(
	sc scenario[models.Store],
) func(*testing.T) {
	return func(
		t *testing.T,
	) {
		sc(
			t,
			store.New(),
		)
	}
}

type recorder struct {
	testing.TB
	failed atomic.Bool
}

// Helper is a no-op so failures point at the scenario.
func (r *recorder) Helper() {}

// Errorf records a failure.
func (r *recorder) Errorf(
	string,
	...any,
) {
	r.failed.Store(true)
}

// Error records a failure.
func (r *recorder) Error(
	...any,
) {
	r.failed.Store(true)
}

// Fatalf records a failure and stops the scenario.
func (r *recorder) Fatalf(
	string,
	...any,
) {
	r.failed.Store(true)
	runtime.Goexit()
}

// Fatal records a failure and stops the scenario.
func (r *recorder) Fatal(
	...any,
) {
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
		go recordScenario(
			&wg,
			&mu,
			failed,
			name,
			check,
			subject,
		)
	}

	wg.Wait()
	return failed
}

func recordScenario[S any](
	wg *sync.WaitGroup,
	mu *sync.Mutex,
	failed map[string]bool,
	name string,
	check scenario[S],
	subject S,
) {
	defer wg.Done()
	didFail := runScenario(
		check,
		subject,
	)
	mu.Lock()
	failed[name] = didFail
	mu.Unlock()
}

func runScenario[S any](
	check scenario[S],
	subject S,
) bool {
	r := &recorder{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		check(
			r,
			subject,
		)
	}()
	<-done
	return r.failed.Load()
}

func flagValue(
	f bool,
) int {
	if f {
		return 1
	}

	return 0
}

func countFailed(
	failed map[string]bool,
) int {
	n := 0
	for _, f := range failed {
		n += flagValue(f)
	}

	return n
}

type hollowStore struct{ models.Store }

// Atomically reports success without running fn.
func (hollowStore) Atomically(
	context.Context,
	func(models.Tx) error,
) error {
	return nil
}

type brokenStore struct{ models.Store }

var errBroken = errors.New("broken")

// Atomically always fails.
func (brokenStore) Atomically(
	context.Context,
	func(models.Tx) error,
) error {
	return errBroken
}

// ReadFrom always fails.
func (brokenStore) ReadFrom(
	context.Context,
	string,
	int64,
) ([][]byte, error) {
	return nil,
		errBroken
}

// ReadRange always fails.
func (brokenStore) ReadRange(
	context.Context,
	string,
	int64,
	int64,
) ([][]byte, error) {
	return nil,
		errBroken
}

// GetReceipt always fails.
func (brokenStore) GetReceipt(
	context.Context,
	string,
) (models.Receipt, bool, error) {
	return models.Receipt{},
		false,
		errBroken
}

// PendingDeliveries always fails.
func (brokenStore) PendingDeliveries(
	context.Context,
	int,
) ([]models.Delivery, error) {
	return nil,
		errBroken
}

// AckDelivery always fails.
func (brokenStore) AckDelivery(
	context.Context,
	string,
	int64,
) error {
	return errBroken
}

func TestSuite_FlagsStoresThatDoNotKeepWhatTheyAccepted(
	t *testing.T,
) {
	failed := failures(
		storeScenarios(),
		models.Store(hollowStore{Store: store.New()}),
	)

	if got, total := countFailed(failed), len(failed); got < total/2 {
		t.Fatalf(
			"only %d of %d scenarios failed a store that keeps nothing",
			got,
			total,
		)
	}

	if !failed["AppendCommitsAndReadFromReturnsInOrder"] {
		t.Fatal("a store that keeps nothing passed the append scenario")
	}
}

func TestSuite_FlagsStoresThatFail(
	t *testing.T,
) {
	failed := failures(
		storeScenarios(),
		models.Store(brokenStore{Store: store.New()}),
	)

	if got, total := countFailed(failed), len(failed); got < total/2 {
		t.Fatalf(
			"only %d of %d scenarios failed a store that errors on everything",
			got,
			total,
		)
	}
}

type forgetfulSnapshots struct{ models.SnapshotStore }

// Put reports success without storing anything.
func (forgetfulSnapshots) Put(
	context.Context,
	string,
	int64,
	[]byte,
) error {
	return nil
}

type failingSnapshots struct{ models.SnapshotStore }

// Put always fails.
func (failingSnapshots) Put(
	context.Context,
	string,
	int64,
	[]byte,
) error {
	return errBroken
}

// Get always fails.
func (failingSnapshots) Get(
	context.Context,
	string,
) ([]byte, bool, error) {
	return nil,
		false,
		errBroken
}

// Delete always fails.
func (failingSnapshots) Delete(
	context.Context,
	string,
) error {
	return errBroken
}

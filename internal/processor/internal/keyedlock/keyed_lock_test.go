package keyedlock

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestKeyedLock_SameKeyIsMutuallyExclusive(
	t *testing.T,
) {
	var k KeyedLock
	var inside, maxInside int
	var mu sync.Mutex
	var wg sync.WaitGroup

	hold := func() {
		defer wg.Done()
		holdExclusively(
			t,
			&k,
			&mu,
			&inside,
			&maxInside,
		)
	}
	for range 50 {
		wg.Add(1)
		go hold()
	}

	wg.Wait()

	if maxInside != 1 {
		t.Fatalf(
			"max concurrent holders = %d, want 1",
			maxInside,
		)
	}

	if n := k.size(); n != 0 {
		t.Fatalf(
			"entries left after all unlocks = %d, want 0",
			n,
		)
	}
}

func holdExclusively(
	t *testing.T,
	k *KeyedLock,
	mu *sync.Mutex,
	inside *int,
	maxInside *int,
) {
	unlock, err := k.Lock(
		context.Background(),
		"agg",
	)
	if err != nil {
		t.Errorf(
			"Lock: %v",
			err,
		)
		return
	}

	mu.Lock()
	*inside++
	*maxInside = max(
		*maxInside,
		*inside,
	)
	mu.Unlock()

	runtime.Gosched()

	mu.Lock()
	*inside--
	mu.Unlock()
	unlock()
}

func TestKeyedLock_DifferentKeysDoNotBlock(
	t *testing.T,
) {
	var k KeyedLock
	unlockA, err := k.Lock(
		context.Background(),
		"a",
	)
	if err != nil {
		t.Fatal(err)
	}

	defer unlockA()

	done := make(chan struct{})
	lockB := func() {
		lockAndRelease(
			&k,
			"b",
		)
		close(done)
	}
	go lockB()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Lock(b) blocked behind Lock(a)")
	}
}

func lockAndRelease(
	k *KeyedLock,
	key string,
) {
	unlock, err := k.Lock(
		context.Background(),
		key,
	)
	if err == nil {
		unlock()
	}
}

func TestKeyedLock_CancelledWaiterReturnsCtxErrAndLeavesNoEntry(
	t *testing.T,
) {
	var k KeyedLock
	unlock, err := k.Lock(
		context.Background(),
		"agg",
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(
		chan error,
		1,
	)
	waiter := func() {
		_, err := k.Lock(
			ctx,
			"agg",
		)
		errCh <- err
	}
	go waiter()
	for k.refs("agg") < 2 {
		runtime.Gosched()
	}

	cancel()

	if err := <-errCh; err != context.Canceled {
		t.Fatalf(
			"waiter err = %v, want context.Canceled",
			err,
		)
	}

	unlock()
	if n := k.size(); n != 0 {
		t.Fatalf(
			"entries left = %d, want 0",
			n,
		)
	}
}

func TestKeyedLock_UnlockIsIdempotent(
	t *testing.T,
) {
	var k KeyedLock
	unlock := mustLock(
		t,
		&k,
		"agg",
	)
	unlock()
	unlock()

	unlock2, err := k.Lock(
		context.Background(),
		"agg",
	)
	if err != nil {
		t.Fatal(err)
	}

	unlock2()
}

func TestKeyedLock_TryLockTakesAFreeKeyAndRefusesAHeldOne(
	t *testing.T,
) {
	var k KeyedLock

	unlock, ok := k.TryLock("agg")
	if !ok {
		t.Fatal("TryLock refused a free key")
	}

	if _, again := k.TryLock("agg"); again {
		t.Fatal("TryLock took a key that is held")
	}

	other, ok := k.TryLock("other")
	if !ok {
		t.Fatal("TryLock refused an unrelated key")
	}

	other()

	unlock()
	unlock()
	retaken, ok := k.TryLock("agg")
	if !ok {
		t.Fatal("TryLock refused the key after it was released")
	}

	retaken()
	if n := k.size(); n != 0 {
		t.Fatalf(
			"entries left after all unlocks = %d, want 0",
			n,
		)
	}
}

func TestKeyedLock_TryLockDoesNotJumpAWaiter(
	t *testing.T,
) {
	var k KeyedLock
	unlock, held := k.TryLock("agg")
	if !held {
		t.Fatal("TryLock refused a free key")
	}

	acquired := make(
		chan func(),
		1,
	)
	acquire := func() {
		handOver(
			&k,
			acquired,
		)
	}
	go acquire()
	for k.refs("agg") < 2 {
		runtime.Gosched()
	}

	unlock()
	release := <-acquired
	if _, ok := k.TryLock("agg"); ok {
		t.Fatal("TryLock took the key from the waiter that was handed it")
	}

	release()
}

func (k *KeyedLock) refs(
	key string,
) int {
	k.mu.Lock()
	defer k.mu.Unlock()
	if e, ok := k.entries[key]; ok {
		return e.refs
	}

	return 0
}

func mustLock(
	t *testing.T,
	k *KeyedLock,
	key string,
) func() {
	t.Helper()
	unlock, err := k.Lock(
		context.Background(),
		key,
	)
	if err != nil {
		t.Fatal(err)
	}

	return unlock
}

func handOver(
	k *KeyedLock,
	acquired chan<- func(),
) {
	release, err := k.Lock(
		context.Background(),
		"agg",
	)
	if err == nil {
		acquired <- release
	}
}

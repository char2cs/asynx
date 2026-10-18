package exec

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestKeyedLock_SameKeyIsMutuallyExclusive(t *testing.T) {
	var k KeyedLock
	var inside, maxInside int
	var mu sync.Mutex
	var wg sync.WaitGroup

	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			unlock, err := k.Lock(context.Background(), "agg")
			if err != nil {
				t.Errorf("Lock: %v", err)
				return
			}
			mu.Lock()
			inside++
			maxInside = max(maxInside, inside)
			mu.Unlock()

			runtime.Gosched() // widen the window for a second holder to show up

			mu.Lock()
			inside--
			mu.Unlock()
			unlock()
		}()
	}
	wg.Wait()

	if maxInside != 1 {
		t.Fatalf("max concurrent holders = %d, want 1", maxInside)
	}
	if n := k.size(); n != 0 {
		t.Fatalf("entries left after all unlocks = %d, want 0", n)
	}
}

func TestKeyedLock_DifferentKeysDoNotBlock(t *testing.T) {
	var k KeyedLock
	unlockA, err := k.Lock(context.Background(), "a")
	if err != nil {
		t.Fatal(err)
	}
	defer unlockA()

	done := make(chan struct{})
	go func() {
		unlockB, err := k.Lock(context.Background(), "b")
		if err == nil {
			unlockB()
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Lock(b) blocked behind Lock(a)")
	}
}

func TestKeyedLock_CancelledWaiterReturnsCtxErrAndLeavesNoEntry(t *testing.T) {
	var k KeyedLock
	unlock, err := k.Lock(context.Background(), "agg")
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := k.Lock(ctx, "agg")
		errCh <- err
	}()
	// Wait until the waiter has registered on the key.
	for {
		k.mu.Lock()
		waiting := k.entries["agg"] != nil && k.entries["agg"].refs == 2
		k.mu.Unlock()
		if waiting {
			break
		}
		runtime.Gosched()
	}
	cancel()

	if err := <-errCh; err != context.Canceled {
		t.Fatalf("waiter err = %v, want context.Canceled", err)
	}

	unlock()
	if n := k.size(); n != 0 {
		t.Fatalf("entries left = %d, want 0", n)
	}
}

func TestKeyedLock_UnlockIsIdempotent(t *testing.T) {
	var k KeyedLock
	unlock, _ := k.Lock(context.Background(), "agg")
	unlock()
	unlock()

	unlock2, err := k.Lock(context.Background(), "agg")
	if err != nil {
		t.Fatal(err)
	}
	unlock2()
}

func TestKeyedLock_TryLockTakesAFreeKeyAndRefusesAHeldOne(t *testing.T) {
	var k KeyedLock

	unlock, ok := k.TryLock("agg")
	if !ok {
		t.Fatal("TryLock refused a free key")
	}
	if _, again := k.TryLock("agg"); again {
		t.Fatal("TryLock took a key that is held")
	}
	if other, ok := k.TryLock("other"); !ok {
		t.Fatal("TryLock refused an unrelated key")
	} else {
		other()
	}

	unlock()
	unlock()
	retaken, ok := k.TryLock("agg")
	if !ok {
		t.Fatal("TryLock refused the key after it was released")
	}
	retaken()
	if n := k.size(); n != 0 {
		t.Fatalf("entries left after all unlocks = %d, want 0", n)
	}
}

func TestKeyedLock_TryLockDoesNotJumpAWaiter(t *testing.T) {
	var k KeyedLock
	unlock, _ := k.TryLock("agg")

	acquired := make(chan func(), 1)
	go func() {
		release, err := k.Lock(context.Background(), "agg")
		if err == nil {
			acquired <- release
		}
	}()
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

// refs reports how many holders and waiters the key has. For tests.
func (k *KeyedLock) refs(key string) int {
	k.mu.Lock()
	defer k.mu.Unlock()
	if e, ok := k.entries[key]; ok {
		return e.refs
	}
	return 0
}

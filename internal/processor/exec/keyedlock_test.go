package exec

import (
	"context"
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

			time.Sleep(100 * time.Microsecond)

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
	time.Sleep(20 * time.Millisecond)
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

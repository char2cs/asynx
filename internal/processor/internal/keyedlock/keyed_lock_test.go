package keyedlock

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"
)

type exclusionProbe struct {
	t         *testing.T
	k         *KeyedLock
	mu        sync.Mutex
	inside    int
	maxInside int
	wg        sync.WaitGroup
}

func (p *exclusionProbe) holdOnce() {
	defer p.wg.Done()
	err := p.k.Lock(
		context.Background(),
		"agg",
	)
	if err != nil {
		p.t.Errorf(
			"Lock: %v",
			err,
		)
		return
	}

	p.mu.Lock()
	p.inside++
	p.maxInside = max(
		p.maxInside,
		p.inside,
	)
	p.mu.Unlock()

	runtime.Gosched()

	p.mu.Lock()
	p.inside--
	p.mu.Unlock()
	p.k.Unlock("agg")
}

func TestKeyedLock_SameKeyIsMutuallyExclusive(
	t *testing.T,
) {
	var k KeyedLock
	probe := &exclusionProbe{
		t: t,
		k: &k,
	}
	for range 50 {
		probe.wg.Add(1)
		go probe.holdOnce()
	}

	probe.wg.Wait()

	if probe.maxInside != 1 {
		t.Fatalf(
			"max concurrent holders = %d, want 1",
			probe.maxInside,
		)
	}

	if n := k.size(); n != 0 {
		t.Fatalf(
			"entries left after all unlocks = %d, want 0",
			n,
		)
	}
}

func TestKeyedLock_DifferentKeysDoNotBlock(
	t *testing.T,
) {
	var k KeyedLock
	err := k.Lock(
		context.Background(),
		"a",
	)
	if err != nil {
		t.Fatal(err)
	}

	defer k.Unlock("a")

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
	err := k.Lock(
		context.Background(),
		key,
	)
	if err == nil {
		k.Unlock(key)
	}
}

func TestKeyedLock_CancelledWaiterReturnsCtxErrAndLeavesNoEntry(
	t *testing.T,
) {
	var k KeyedLock
	err := k.Lock(
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
		errCh <- k.Lock(
			ctx,
			"agg",
		)
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

	k.Unlock("agg")
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
	mustLock(
		t,
		&k,
		"agg",
	)
	k.Unlock("agg")
	k.Unlock("agg")

	err := k.Lock(
		context.Background(),
		"agg",
	)
	if err != nil {
		t.Fatal(err)
	}

	k.Unlock("agg")
}

func TestKeyedLock_TryLockTakesAFreeKeyAndRefusesAHeldOne(
	t *testing.T,
) {
	var k KeyedLock

	if err := k.TryLock("agg"); err != nil {
		t.Fatal("TryLock refused a free key")
	}

	if err := k.TryLock("agg"); err == nil {
		t.Fatal("TryLock took a key that is held")
	}

	if err := k.TryLock("other"); err != nil {
		t.Fatal("TryLock refused an unrelated key")
	}

	k.Unlock("other")

	k.Unlock("agg")
	k.Unlock("agg")
	if err := k.TryLock("agg"); err != nil {
		t.Fatal("TryLock refused the key after it was released")
	}

	k.Unlock("agg")
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
	if err := k.TryLock("agg"); err != nil {
		t.Fatal("TryLock refused a free key")
	}

	acquired := make(
		chan struct{},
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

	k.Unlock("agg")
	<-acquired
	if err := k.TryLock("agg"); err == nil {
		t.Fatal("TryLock took the key from the waiter that was handed it")
	}

	k.Unlock("agg")
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
) {
	t.Helper()
	err := k.Lock(
		context.Background(),
		key,
	)
	if err != nil {
		t.Fatal(err)
	}
}

func handOver(
	k *KeyedLock,
	acquired chan<- struct{},
) {
	err := k.Lock(
		context.Background(),
		"agg",
	)
	if err == nil {
		acquired <- struct{}{}
	}
}

package keyedlock

import (
	"context"
	"sync"
)

// KeyedLock is a mutex per string key. Same-key holders are mutually
// exclusive; different keys never block each other. An entry exists only while
// someone holds or waits for its key, so memory does not grow with the number
// of distinct keys. The zero value is ready to use and the lock is safe for
// concurrent use. It is not reentrant: a holder that locks its own key again
// blocks forever, so callers must never lock a key from inside a section that
// already holds it. Waiters are served in no guaranteed order. The executor
// uses one KeyedLock to serialise every command, forget and resume of an
// aggregate.
type KeyedLock struct {
	mu      sync.Mutex
	entries map[string]*lockEntry
}

// Lock blocks until the key is free or ctx is done. On success the returned
// unlock releases the key; calling it more than once is a no-op, so it is safe
// to defer and to call early. If ctx is already done, or ends while waiting,
// Lock returns ctx.Err() and the key is untouched: the waiter leaves no entry
// behind and never holds the key. Lock is the only way to wait for a key; a
// holder must release it from any goroutine exactly as it would a sync.Mutex.
func (k *KeyedLock) Lock(
	ctx context.Context,
	key string,
) (unlock func(), err error) {
	if err := ctx.Err(); err != nil {
		return nil,
			err
	}

	k.join(key)
	e := k.entryOf(key)
	select {
	case <-e.ch:
	case <-ctx.Done():
		k.leave(
			key,
			e,
		)
		return nil,
			ctx.Err()
	}

	return k.unlocker(
			key,
			e,
		),
		nil
}

// TryLock takes the key if it is free right now and reports whether it did,
// like sync.Mutex.TryLock. It never waits, and it does not jump a waiter that
// was already handed the key. On success the returned unlock releases the key
// and is idempotent as in Lock; on failure unlock is nil and nothing is held.
//
// TryLock is the one operation that both mutates and answers: taking a free
// key and learning that it was free is a single atomic test-and-set, and
// splitting it into a query and a command would let two callers both see the
// key free. The executor's WithLockIfFree is its only caller.
func (k *KeyedLock) TryLock(
	key string,
) (unlock func(), ok bool) {
	k.join(key)
	e := k.entryOf(key)
	select {
	case <-e.ch:
		return k.unlocker(
				key,
				e,
			),
			true
	default:
		k.leave(
			key,
			e,
		)
		return nil,
			false
	}
}

func (k *KeyedLock) join(
	key string,
) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.entries == nil {
		k.entries = make(map[string]*lockEntry)
	}

	if _, exists := k.entries[key]; !exists {
		k.entries[key] = newLockEntry()
	}

	k.entries[key].refs++
}

func newLockEntry() *lockEntry {
	e := &lockEntry{ch: make(
		chan struct{},
		1,
	)}
	e.ch <- struct{}{}
	return e
}

func (k *KeyedLock) entryOf(
	key string,
) *lockEntry {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.entries[key]
}

func (k *KeyedLock) unlocker(
	key string,
	e *lockEntry,
) func() {
	var once sync.Once
	free := func() {
		e.ch <- struct{}{}
		k.leave(
			key,
			e,
		)
	}
	return func() {
		once.Do(free)
	}
}

func (k *KeyedLock) leave(
	key string,
	e *lockEntry,
) {
	k.mu.Lock()
	defer k.mu.Unlock()
	e.refs--
	if e.refs == 0 {
		delete(
			k.entries,
			key,
		)
	}
}

func (k *KeyedLock) size() int {
	k.mu.Lock()
	defer k.mu.Unlock()
	return len(k.entries)
}

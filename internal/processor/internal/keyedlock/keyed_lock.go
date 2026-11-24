package keyedlock

import (
	"context"
	"errors"
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

// Lock blocks until the key is free or ctx is done. On success the caller
// holds the key and must release it with Unlock. If ctx is already done, or
// ends while waiting, Lock returns ctx.Err() and the key is untouched: the
// waiter leaves no entry behind and never holds the key. Lock is the only way
// to wait for a key; a holder may release it from any goroutine exactly as it
// would a sync.Mutex.
func (k *KeyedLock) Lock(
	ctx context.Context,
	key string,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	k.join(key)
	e := k.entryOf(key)
	select {
	case <-e.ch:
		return nil
	case <-ctx.Done():
		k.leave(
			key,
			e,
		)
		return ctx.Err()
	}
}

// ErrHeld is what TryLock returns when the key is held by someone else.
var ErrHeld = errors.New("keyedlock: the key is held")

// TryLock takes the key if it is free right now and returns ErrHeld when it is
// not, like sync.Mutex.TryLock. It never waits, and it does not jump a waiter
// that was already handed the key. On success the caller holds the key and
// releases it with Unlock; on failure nothing is held. The executor's
// WithLockIfFree is its only caller.
func (k *KeyedLock) TryLock(
	key string,
) error {
	k.join(key)
	e := k.entryOf(key)
	select {
	case <-e.ch:
		return nil
	default:
		k.leave(
			key,
			e,
		)
		return ErrHeld
	}
}

// Unlock releases a key taken by Lock or TryLock, waking the next waiter if
// there is one. Unlocking a key that is not held, for instance a second time
// right after the first, is a no-op, so it is safe to defer and to call early;
// the holder must still release exactly once, as another holder may have taken
// the key in between.
func (k *KeyedLock) Unlock(
	key string,
) {
	k.mu.Lock()
	defer k.mu.Unlock()
	e := k.entries[key]
	if e == nil ||
		len(e.ch) > 0 {
		return
	}

	e.ch <- struct{}{}
	k.drop(
		key,
		e,
	)
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

func (k *KeyedLock) leave(
	key string,
	e *lockEntry,
) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.drop(
		key,
		e,
	)
}

func (k *KeyedLock) drop(
	key string,
	e *lockEntry,
) {
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

package keyedlock

import (
	"context"
	"sync"
)

// KeyedLock is a mutex per string key. Same-key holders are mutually
// exclusive; different keys never block each other. An entry exists only while
// someone holds or waits for its key, so memory does not grow with the number
// of distinct keys. The zero value is ready to use.
type KeyedLock struct {
	mu      sync.Mutex
	entries map[string]*lockEntry
}

// Lock blocks until the key is free or ctx is done. On success the returned
// unlock releases the key; calling it more than once is a no-op.
func (k *KeyedLock) Lock(
	ctx context.Context,
	key string,
) (unlock func(), err error) {
	if err := ctx.Err(); err != nil {
		return nil,
			err
	}

	k.mu.Lock()
	k.ensure(key)
	e := k.entries[key]
	retain(e)
	k.mu.Unlock()

	select {
	case <-e.ch:
	case <-ctx.Done():
		k.release(
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

// TryLock takes the key if it is free right now and reports whether it did. It
// never waits.
func (k *KeyedLock) TryLock(
	key string,
) (unlock func(), ok bool) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.ensure(key)
	e := k.entries[key]
	select {
	case <-e.ch:
		retain(e)
		return k.unlocker(
				key,
				e,
			),
			true
	default:
		return nil,
			false
	}
}

func (k *KeyedLock) ensure(
	key string,
) {
	if k.entries == nil {
		k.entries = make(map[string]*lockEntry)
	}

	if _, exists := k.entries[key]; exists {
		return
	}

	e := &lockEntry{ch: make(
		chan struct{},
		1,
	)}
	e.ch <- struct{}{}
	k.entries[key] = e
}

func retain(
	e *lockEntry,
) {
	e.refs++
}

func (k *KeyedLock) unlocker(
	key string,
	e *lockEntry,
) func() {
	var once sync.Once
	free := func() {
		e.ch <- struct{}{}
		k.release(
			key,
			e,
		)
	}
	return func() {
		once.Do(free)
	}
}

func (k *KeyedLock) release(
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

package bus

import (
	"context"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/char2cs/asynx"
	"github.com/char2cs/asynx/utils"
)

type subscription[T any] struct {
	pattern         string
	handler         func(asynx.Event[T])
	fallbackHandler func(asynx.Event[T])
	timeout         time.Duration
}

// publishIndex is an immutable snapshot of the subscription indexes.
// Once stored via atomic.Pointer it is never modified; Subscribe and
// Unsubscribe always build and store a brand-new instance (copy-on-write).
type publishIndex[T any] struct {
	exactSubs map[string][]*subscription[T] // pattern → subs (exact-match index)
	regexSubs []*subscription[T]            // subs whose pattern has metacharacters
}

// ChannelBus is an in-process event bus that dispatches events to subscribers
// via goroutines. It implements asynx.Bus[T].
//
// Hot path (Publish) is lock-free for index reads: it atomically loads an
// immutable snapshot of subscriptions and iterates it without holding any mutex.
// The only brief critical section in Publish is the closed-check + WaitGroup.Add,
// which uses a separate RWMutex held for nanoseconds.
//
// Subscribe and Unsubscribe mutate the index under an exclusive mutex and store
// a freshly-built publishIndex atomically, so in-flight Publish calls reading
// the old snapshot are never disturbed.
type ChannelBus[T any] struct {
	// Subscription management — exclusive, infrequent.
	subMu         sync.Mutex
	subscriptions map[string]*subscription[T] // subID → sub (for Unsubscribe)
	idx           atomic.Pointer[publishIndex[T]]

	// Publish / Close coordination.
	// publishMu is held for RLock during the closed-check + Add phase of Publish,
	// and for Lock by Close. This ensures Add is never called after Wait returns.
	publishMu  sync.RWMutex
	closed     bool
	inFlightWg sync.WaitGroup

	// Pattern compilation cache — separate lock, unrelated to subscriptions.
	patternMu        sync.RWMutex
	compiledPatterns map[string]*regexp.Regexp
}

// NewChannelBus returns an initialized ChannelBus ready for use.
func NewChannelBus[T any]() *ChannelBus[T] {
	b := &ChannelBus[T]{
		subscriptions:    make(map[string]*subscription[T]),
		compiledPatterns: make(map[string]*regexp.Regexp),
	}
	b.idx.Store(&publishIndex[T]{
		exactSubs: make(map[string][]*subscription[T]),
	})
	return b
}

func (b *ChannelBus[T]) Subscribe(
	pattern string,
	handler func(asynx.Event[T]),
	opts ...asynx.SubscriptionOpt[T],
) (subID string, err error) {
	if pattern == "" {
		return "", asynx.ErrEmptyPattern
	}
	if handler == nil {
		return "", asynx.ErrNilHandler
	}

	cfg := &asynx.SubscriptionConfig[T]{}
	for _, opt := range opts {
		opt(cfg)
	}

	b.subMu.Lock()
	defer b.subMu.Unlock()

	b.publishMu.RLock()
	closed := b.closed
	b.publishMu.RUnlock()

	if closed {
		return "", asynx.ErrBusClosed
	}

	subID = utils.NewID()
	sub := &subscription[T]{
		pattern:         pattern,
		handler:         handler,
		fallbackHandler: cfg.Fallback,
		timeout:         cfg.Timeout,
	}

	b.subscriptions[subID] = sub
	b.idx.Store(indexWith(b.idx.Load(), sub))

	return subID, nil
}

func (b *ChannelBus[T]) Unsubscribe(id string) error {
	b.subMu.Lock()
	defer b.subMu.Unlock()

	sub, ok := b.subscriptions[id]
	if !ok {
		return nil
	}

	delete(b.subscriptions, id)
	b.idx.Store(indexWithout(b.idx.Load(), sub))

	return nil
}

func (b *ChannelBus[T]) Publish(ctx context.Context, event asynx.Event[T]) error {
	// Step 1 — lock-free: load immutable snapshot, find matching subscriptions.
	idx := b.idx.Load()

	exactMatches := idx.exactSubs[event.EventName]

	var regexMatches []*subscription[T]
	for _, sub := range idx.regexSubs {
		if b.matchesRegex(event.EventName, sub.pattern) {
			regexMatches = append(regexMatches, sub)
		}
	}

	total := len(exactMatches) + len(regexMatches)

	// Step 2 — brief critical section: closed-check + WaitGroup.Add must be
	// atomic with respect to Close, so that Add is never called after Wait returns.
	b.publishMu.RLock()
	if b.closed {
		b.publishMu.RUnlock()
		return asynx.ErrBusClosed
	}
	if total > 0 {
		b.inFlightWg.Add(total)
	}
	b.publishMu.RUnlock()

	// Step 3 — lock-free: spawn one goroutine per matching handler.
	for _, sub := range exactMatches {
		go b.executeHandler(makeJob(sub, event))
	}
	for _, sub := range regexMatches {
		go b.executeHandler(makeJob(sub, event))
	}

	return nil
}

func (b *ChannelBus[T]) Close(ctx context.Context) error {
	// Holding publishMu.Lock ensures all in-flight Publish calls that passed
	// the closed-check have already called Add before we call Wait.
	b.publishMu.Lock()
	b.closed = true
	b.publishMu.Unlock()

	done := make(chan struct{})
	go func() {
		b.inFlightWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// indexWith returns a new publishIndex with sub added. The caller must hold subMu.
func indexWith[T any](old *publishIndex[T], sub *subscription[T]) *publishIndex[T] {
	if isExactPattern(sub.pattern) {
		existing := old.exactSubs[sub.pattern]
		newSlice := make([]*subscription[T], len(existing)+1)
		copy(newSlice, existing)
		newSlice[len(existing)] = sub

		newExact := make(map[string][]*subscription[T], len(old.exactSubs)+1)
		for k, v := range old.exactSubs {
			newExact[k] = v
		}
		newExact[sub.pattern] = newSlice

		return &publishIndex[T]{exactSubs: newExact, regexSubs: old.regexSubs}
	}

	newRegex := make([]*subscription[T], len(old.regexSubs)+1)
	copy(newRegex, old.regexSubs)
	newRegex[len(old.regexSubs)] = sub

	return &publishIndex[T]{exactSubs: old.exactSubs, regexSubs: newRegex}
}

// indexWithout returns a new publishIndex with sub removed. The caller must hold subMu.
func indexWithout[T any](old *publishIndex[T], sub *subscription[T]) *publishIndex[T] {
	if isExactPattern(sub.pattern) {
		existing := old.exactSubs[sub.pattern]
		newSlice := make([]*subscription[T], 0, len(existing)-1)
		for _, s := range existing {
			if s != sub {
				newSlice = append(newSlice, s)
			}
		}

		newExact := make(map[string][]*subscription[T], len(old.exactSubs))
		for k, v := range old.exactSubs {
			newExact[k] = v
		}
		if len(newSlice) == 0 {
			delete(newExact, sub.pattern)
		} else {
			newExact[sub.pattern] = newSlice
		}

		return &publishIndex[T]{exactSubs: newExact, regexSubs: old.regexSubs}
	}

	newRegex := make([]*subscription[T], 0, len(old.regexSubs)-1)
	for _, s := range old.regexSubs {
		if s != sub {
			newRegex = append(newRegex, s)
		}
	}

	return &publishIndex[T]{exactSubs: old.exactSubs, regexSubs: newRegex}
}

func makeJob[T any](sub *subscription[T], event asynx.Event[T]) *handlerJob[T] {
	return &handlerJob[T]{
		handler:         sub.handler,
		fallbackHandler: sub.fallbackHandler,
		timeout:         sub.timeout,
		event:           event,
	}
}

func (b *ChannelBus[T]) matchesRegex(eventName, pattern string) bool {
	compiled, err := b.getCompiledPattern(pattern)
	if err != nil {
		return false
	}
	return compiled.MatchString(eventName)
}

func (b *ChannelBus[T]) getCompiledPattern(pattern string) (*regexp.Regexp, error) {
	b.patternMu.RLock()
	if compiled, ok := b.compiledPatterns[pattern]; ok {
		b.patternMu.RUnlock()
		return compiled, nil
	}
	b.patternMu.RUnlock()

	compiled, err := regexp.Compile(pattern)
	if err != nil {
		return nil, err
	}

	b.patternMu.Lock()
	defer b.patternMu.Unlock()

	if existing, ok := b.compiledPatterns[pattern]; ok {
		return existing, nil
	}

	b.compiledPatterns[pattern] = compiled
	return compiled, nil
}

func isExactPattern(pattern string) bool {
	return regexp.QuoteMeta(pattern) == pattern
}

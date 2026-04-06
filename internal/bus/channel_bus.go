// ChannelBus is an in-process event bus that dispatches events to subscribers
// via goroutines. It implements asynx.Bus[T].
//
// Hot path (Publish) is lock-free for index reads: it atomically loads an
// immutable snapshot of subscriptions and iterates it without holding any mutex.
// The only brief critical section in Publish is the closed-check + WaitGroup.Add,
// which uses a separate RWMutex held for nanoseconds.
//
// Subscribe and Unsubscribe mutate the index under an exclusive mutex and store
// a freshly-built PublishIndex atomically, so in-flight Publish calls reading
// the old snapshot are never disturbed.
package bus

import (
	"context"
	"regexp"
	"sync"
	"sync/atomic"

	"github.com/char2cs/asynx/internal/bus/exec"
	"github.com/char2cs/asynx/internal/bus/models"
	"github.com/char2cs/asynx/internal/bus/utils"
	asynxutils "github.com/char2cs/asynx/internal/utils"
	asynxmd "github.com/char2cs/asynx/models"
)

// ChannelBusOpt is a functional option for NewChannelBus.
type ChannelBusOpt[T any] func(*ChannelBus[T])

// WithPanicHandler sets a custom callback invoked when a subscription handler
// panics. When not set, the package-level exec.OnHandlerPanic is used.
func WithPanicHandler[T any](fn asynxmd.PanicHandler[T]) ChannelBusOpt[T] {
	return func(b *ChannelBus[T]) {
		b.onPanic = fn
	}
}

// WithTimeoutHandler sets a custom callback invoked when a subscription handler
// exceeds its timeout. When not set, the package-level exec.OnHandlerTimeout is used.
func WithTimeoutHandler[T any](fn asynxmd.TimeoutHandler[T]) ChannelBusOpt[T] {
	return func(b *ChannelBus[T]) {
		b.onTimeout = fn
	}
}

type ChannelBus[T any] struct {
	// Subscription management — exclusive, infrequent.
	subMu         sync.Mutex
	subscriptions map[string]*models.Subscription[T] // subID → sub (for Unsubscribe)
	idx           atomic.Pointer[models.PublishIndex[T]]

	// Publish / Close coordination.
	// publishMu is held for RLock during the closed-check + Add phase of Publish,
	// and for Lock by Close. This ensures Add is never called after Wait returns.
	publishMu  sync.RWMutex
	closed     bool
	inFlightWg sync.WaitGroup

	// Pattern compilation cache — separate lock, unrelated to subscriptions.
	patternMu        sync.RWMutex
	compiledPatterns map[string]*regexp.Regexp
	patternErrors    map[string]error // Caches compilation errors to avoid recompilation

	// Optional bus-level callback overrides passed to every HandlerJob.
	onPanic   asynxmd.PanicHandler[T]
	onTimeout asynxmd.TimeoutHandler[T]
}

// NewChannelBus returns an initialized ChannelBus ready for use.
func NewChannelBus[T any](opts ...ChannelBusOpt[T]) *ChannelBus[T] {
	b := &ChannelBus[T]{
		subscriptions:    make(map[string]*models.Subscription[T]),
		compiledPatterns: make(map[string]*regexp.Regexp),
		patternErrors:    make(map[string]error),
	}
	b.idx.Store(&models.PublishIndex[T]{
		ExactSubs: make(map[string][]*models.Subscription[T]),
	})
	for _, opt := range opts {
		opt(b)
	}
	return b
}

func (b *ChannelBus[T]) Subscribe(
	pattern string,
	handler asynxmd.ProjectionHandler[T],
	opts ...asynxmd.SubscriptionOpt[T],
) (subID string, err error) {
	if pattern == "" {
		return "", asynxmd.ErrEmptyPattern
	}
	if handler == nil {
		return "", asynxmd.ErrNilHandler
	}

	cfg := &asynxmd.SubscriptionConfig[T]{}
	for _, opt := range opts {
		opt(cfg)
	}

	b.subMu.Lock()
	defer b.subMu.Unlock()

	b.publishMu.RLock()
	closed := b.closed
	b.publishMu.RUnlock()

	if closed {
		return "", asynxmd.ErrBusClosed
	}

	subID = asynxutils.NewID()
	sub := &models.Subscription[T]{
		Pattern:         pattern,
		Handler:         handler,
		FallbackHandler: cfg.Fallback,
		Timeout:         cfg.Timeout,
	}

	b.subscriptions[subID] = sub
	b.idx.Store(utils.IndexWith(b.idx.Load(), sub))

	return subID, nil
}

func (b *ChannelBus[T]) Unsubscribe(
	id string,
) error {
	b.subMu.Lock()
	defer b.subMu.Unlock()

	sub, ok := b.subscriptions[id]
	if !ok {
		return nil
	}

	delete(b.subscriptions, id)
	b.idx.Store(utils.IndexWithout(b.idx.Load(), sub))

	return nil
}

// matchedSubs returns all subscriptions that match eventName, split into exact
// and regex buckets. Lock-free: reads from the immutable atomic snapshot.
func (b *ChannelBus[T]) matchedSubs(
	eventName string,
) (exact, regex []*models.Subscription[T]) {
	idx := b.idx.Load()
	exact = idx.ExactSubs[eventName]
	for _, sub := range idx.RegexSubs {
		if b.matchesRegex(eventName, sub.Pattern) {
			regex = append(regex, sub)
		}
	}
	return exact, regex
}

func (b *ChannelBus[T]) Publish(
	ctx context.Context,
	event asynxmd.Event[T],
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	exactMatches, regexMatches := b.matchedSubs(event.EventName)
	total := len(exactMatches) + len(regexMatches)

	b.publishMu.RLock()
	if b.closed {
		b.publishMu.RUnlock()
		return asynxmd.ErrBusClosed
	}
	if total > 0 {
		b.inFlightWg.Add(total)
	}
	b.publishMu.RUnlock()

	for _, sub := range exactMatches {
		go exec.ExecuteHandler(&b.inFlightWg, utils.MakeJob(
			sub,
			event,
			ctx,
			b.onPanic,
			b.onTimeout,
		))
	}

	for _, sub := range regexMatches {
		go exec.ExecuteHandler(&b.inFlightWg, utils.MakeJob(
			sub,
			event,
			ctx,
			b.onPanic,
			b.onTimeout,
		))
	}

	return nil
}

// PublishSync fires all matching handlers and blocks until every handler
// goroutine triggered by this event has completed. Handler panics and timeouts
// are still handled by PanicHandler/TimeoutHandler — they do not affect the
// returned error.
// Subscription matching uses an immutable atomic snapshot taken before the lock
// acquisition; a concurrent Subscribe that arrives between the snapshot and the
// lock may not have its handler invoked for this event, which is consistent with
// Publish's behavior.
func (b *ChannelBus[T]) PublishSync(
	ctx context.Context,
	event asynxmd.Event[T],
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	exactMatches, regexMatches := b.matchedSubs(event.EventName)
	total := len(exactMatches) + len(regexMatches)

	b.publishMu.RLock()
	if b.closed {
		b.publishMu.RUnlock()
		return asynxmd.ErrBusClosed
	}
	var localWg sync.WaitGroup
	if total > 0 {
		b.inFlightWg.Add(total)
		localWg.Add(total)
	}
	b.publishMu.RUnlock()

	for _, sub := range exactMatches {
		go func(s *models.Subscription[T]) {
			defer localWg.Done()
			exec.ExecuteHandler(&b.inFlightWg, utils.MakeJob(
				s,
				event,
				ctx,
				b.onPanic,
				b.onTimeout,
			))
		}(sub)
	}
	for _, sub := range regexMatches {
		go func(s *models.Subscription[T]) {
			defer localWg.Done()
			exec.ExecuteHandler(&b.inFlightWg, utils.MakeJob(
				s,
				event,
				ctx,
				b.onPanic,
				b.onTimeout,
			))
		}(sub)
	}

	localWg.Wait()
	return nil
}

func (b *ChannelBus[T]) Close(
	ctx context.Context,
) error {
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

func (b *ChannelBus[T]) matchesRegex(
	eventName,
	pattern string,
) bool {
	compiled, err := b.getCompiledPattern(pattern)
	if err != nil {
		return false
	}
	return compiled.MatchString(eventName)
}

func (b *ChannelBus[T]) getCompiledPattern(
	pattern string,
) (*regexp.Regexp, error) {
	b.patternMu.RLock()

	if compiled, ok := b.compiledPatterns[pattern]; ok {
		b.patternMu.RUnlock()
		return compiled, nil
	}

	if err, cached := b.patternErrors[pattern]; cached {
		b.patternMu.RUnlock()
		return nil, err
	}

	b.patternMu.RUnlock()

	compiled, err := regexp.Compile(pattern)
	if err != nil {
		b.patternMu.Lock()
		b.patternErrors[pattern] = err
		b.patternMu.Unlock()
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

// WaitForHandlers blocks until all in-flight handler executions complete.
// Only for use in tests; do not call in production code.
func (b *ChannelBus[T]) WaitForHandlers() {
	b.inFlightWg.Wait()
}

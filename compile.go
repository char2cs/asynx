package asynx

import (
	"fmt"
	"github.com/char2cs/asynx/v2/internal/bus"
	procmodels "github.com/char2cs/asynx/v2/internal/processor/models"
	"github.com/char2cs/asynx/v2/models/failure"
	"github.com/char2cs/asynx/v2/models/subscription"
	"regexp"
)

func (b *Builder[T]) compileSyncProjections() ([]procmodels.SyncProjection[T], error) {
	return compileAll(
		b.syncProjections,
		compileSyncProjection[T],
	)
}

func compileAll[D any, R any](
	defs []D,
	compile func(int, D) (R, error),
) (compiled []R, err error) {
	compiled = make(
		[]R,
		0,
		len(defs),
	)
	for i := 0; i < len(defs) &&
		err == nil; i++ {
		var next R
		next, err = compile(
			i,
			defs[i],
		)
		compiled = append(
			compiled,
			next,
		)
	}

	if err != nil {
		return nil,
			err
	}

	return compiled,
		nil
}

func compileSyncProjection[T any](
	_ int,
	def syncProjectionDef[T],
) (procmodels.SyncProjection[T], error) {
	if def.pattern == "" {
		return procmodels.SyncProjection[T]{},
			failure.ErrEmptyPattern
	}

	if def.fn == nil {
		return procmodels.SyncProjection[T]{},
			failure.ErrNilHandler
	}

	match, err := matcher(def.pattern)
	if err != nil {
		return procmodels.SyncProjection[T]{},
			err
	}

	return procmodels.SyncProjection[T]{Match: match, Fn: def.fn},
		nil
}

func (b *Builder[T]) compileDurable() ([]subscription.Durable[T], func(string) bool, error) {
	if len(b.subscribers) == 0 {
		return nil,
			nil,
			nil
	}

	subs, err := b.compileSubscribers()
	if err != nil {
		return nil,
			nil,
			err
	}

	return subs,
		durableEvents[T](subs).wants,
		nil
}

func (b *Builder[T]) compileSubscribers() ([]subscription.Durable[T], error) {
	return compileAll(
		b.subscribers,
		compileSubscriber[T],
	)
}

func compileSubscriber[T any](
	index int,
	def subscriberDef[T],
) (subscription.Durable[T], error) {
	if def.pattern == "" {
		return subscription.Durable[T]{},
			failure.ErrEmptyPattern
	}

	if def.handler == nil {
		return subscription.Durable[T]{},
			failure.ErrNilHandler
	}

	cfg := &subscription.Config[T]{}
	for _, opt := range def.opts {
		opt(cfg)
	}

	if cfg.Fallback != nil {
		return subscription.Durable[T]{},
			failure.ErrFallbackUnsupported
	}

	m, err := matcher(def.pattern)
	if err != nil {
		return subscription.Durable[T]{},
			err
	}

	name := fmt.Sprintf(
		"%s#%d",
		def.pattern,
		index+1,
	)
	return subscription.Durable[T]{Name: name, Match: m, Handler: def.handler, Timeout: cfg.Timeout},
		nil
}

func matcher(
	pattern string,
) (func(string) bool, error) {
	if bus.IsExactPattern(pattern) {
		return exactPattern(pattern).matches,
			nil
	}

	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil,
			fmt.Errorf(
				"asynx: invalid pattern %q: %w",
				pattern,
				err,
			)
	}

	return re.MatchString,
		nil
}

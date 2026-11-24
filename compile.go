package asynx

import (
	"fmt"
	"github.com/char2cs/asynx/internal/bus"
	procmodels "github.com/char2cs/asynx/internal/processor/models"
	"github.com/char2cs/asynx/models"
	"regexp"
	"slices"
)

func (b *Builder[T]) compileSyncProjections() ([]procmodels.SyncProjection[T], error) {
	out := make(
		[]procmodels.SyncProjection[T],
		0,
		len(b.syncProjections),
	)
	for _, def := range b.syncProjections {
		sp, err := compileSyncProjection(def)
		if err != nil {
			return nil,
				err
		}

		out = append(
			out,
			sp,
		)
	}

	return out,
		nil
}

func compileSyncProjection[T any](
	def syncProjectionDef[T],
) (procmodels.SyncProjection[T], error) {
	if def.pattern == "" {
		return procmodels.SyncProjection[T]{},
			models.ErrEmptyPattern
	}

	if def.fn == nil {
		return procmodels.SyncProjection[T]{},
			models.ErrNilHandler
	}

	match, err := matcher(def.pattern)
	if err != nil {
		return procmodels.SyncProjection[T]{},
			err
	}

	return procmodels.SyncProjection[T]{Match: match, Fn: def.fn},
		nil
}

func (b *Builder[T]) compileDurable() ([]models.DurableSub[T], func(string) bool, error) {
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
		wantedBy(subs),
		nil
}

func (b *Builder[T]) compileSubscribers() ([]models.DurableSub[T], error) {
	subs := make(
		[]models.DurableSub[T],
		0,
		len(b.subscribers),
	)
	for i, def := range b.subscribers {
		sub, err := compileSubscriber(
			i,
			def,
		)
		if err != nil {
			return nil,
				err
		}

		subs = append(
			subs,
			sub,
		)
	}

	return subs,
		nil
}

func compileSubscriber[T any](
	index int,
	def subscriberDef[T],
) (models.DurableSub[T], error) {
	if def.pattern == "" {
		return models.DurableSub[T]{},
			models.ErrEmptyPattern
	}

	if def.handler == nil {
		return models.DurableSub[T]{},
			models.ErrNilHandler
	}

	cfg := &models.SubscriptionConfig[T]{}
	for _, opt := range def.opts {
		opt(cfg)
	}

	if cfg.Fallback != nil {
		return models.DurableSub[T]{},
			models.ErrFallbackUnsupported
	}

	m, err := matcher(def.pattern)
	if err != nil {
		return models.DurableSub[T]{},
			err
	}

	name := fmt.Sprintf(
		"%s#%d",
		def.pattern,
		index+1,
	)
	return models.DurableSub[T]{Name: name, Match: m, Handler: def.handler, Timeout: cfg.Timeout},
		nil
}

func wantedBy[T any](
	subs []models.DurableSub[T],
) func(string) bool {
	return func(
		name string,
	) bool {
		return slices.ContainsFunc(
			subs,
			func(
				sub models.DurableSub[T],
			) bool {
				return sub.Match(name)
			},
		)
	}
}

func matcher(
	pattern string,
) (func(string) bool, error) {
	if bus.IsExactPattern(pattern) {
		return func(
				name string,
			) bool {
				return name == pattern
			},
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

package routing

import (
	"maps"
	"regexp"
	"slices"

	"github.com/char2cs/asynx/v2/internal/bus/models"
)

// IsExactPattern reports whether pattern contains no regexp metacharacters,
// i.e. it can be matched with a plain string equality check. The empty pattern
// is exact. The result decides which half of a PublishIndex a subscription is
// stored in, so Subscribe and Unsubscribe must classify a pattern identically.
func IsExactPattern(
	pattern string,
) bool {
	return regexp.QuoteMeta(pattern) == pattern
}

// IndexWith returns a new PublishIndex with sub added; old is left untouched,
// so readers still holding it keep a consistent view. Callers must serialise
// IndexWith and IndexWithout (the bus does so with its subscription mutex),
// otherwise concurrent updates built from the same old index lose one another.
// An exact pattern is appended after its existing subscriptions, a regex
// pattern after the existing regex subscriptions, so delivery order within a
// pattern is registration order.
//
// For exact-match patterns this clones the entire ExactSubs map (O(n) in the
// number of distinct patterns), which is the deliberate cost of keeping Publish
// completely lock-free. Acceptable when Subscribe is rare relative to Publish;
// revisit if high-frequency dynamic subscription churn becomes a requirement.
func IndexWith[T any](
	old *models.PublishIndex[T],
	sub *models.Subscription[T],
) *models.PublishIndex[T] {
	if !IsExactPattern(sub.Pattern) {
		newRegex := make(
			[]*models.Subscription[T],
			len(old.RegexSubs)+1,
		)
		copy(
			newRegex,
			old.RegexSubs,
		)
		newRegex[len(old.RegexSubs)] = sub
		return &models.PublishIndex[T]{ExactSubs: old.ExactSubs, RegexSubs: newRegex}
	}

	existing := old.ExactSubs[sub.Pattern]
	newSlice := make(
		[]*models.Subscription[T],
		len(existing)+1,
	)
	copy(
		newSlice,
		existing,
	)
	newSlice[len(existing)] = sub

	newExact := make(
		map[string][]*models.Subscription[T],
		len(old.ExactSubs)+1,
	)
	maps.Copy(
		newExact,
		old.ExactSubs,
	)
	newExact[sub.Pattern] = newSlice

	return &models.PublishIndex[T]{ExactSubs: newExact, RegexSubs: old.RegexSubs}
}

// IndexWithout returns a new PublishIndex with sub removed, matched by pointer
// identity; old is left untouched. A subscription that is not in old yields an
// equal copy, and an exact pattern left without subscriptions disappears from
// the map. The serialisation requirement of IndexWith applies.
func IndexWithout[T any](
	old *models.PublishIndex[T],
	sub *models.Subscription[T],
) *models.PublishIndex[T] {
	if !IsExactPattern(sub.Pattern) {
		newRegex := withoutSub(
			old.RegexSubs,
			sub,
		)
		return &models.PublishIndex[T]{
			ExactSubs: old.ExactSubs,
			RegexSubs: newRegex,
		}
	}

	existing := old.ExactSubs[sub.Pattern]
	newSlice := withoutSub(
		existing,
		sub,
	)

	newExact := make(
		map[string][]*models.Subscription[T],
		len(old.ExactSubs),
	)
	maps.Copy(
		newExact,
		old.ExactSubs,
	)
	delete(
		newExact,
		sub.Pattern,
	)
	if len(newSlice) > 0 {
		newExact[sub.Pattern] = newSlice
	}

	return &models.PublishIndex[T]{
		ExactSubs: newExact,
		RegexSubs: old.RegexSubs,
	}
}

func withoutSub[T any](
	subs []*models.Subscription[T],
	sub *models.Subscription[T],
) []*models.Subscription[T] {
	kept := slices.Clone(subs)
	at := slices.Index(
		kept,
		sub,
	)
	if at < 0 {
		return kept
	}

	return slices.Delete(
		kept,
		at,
		at+1,
	)
}

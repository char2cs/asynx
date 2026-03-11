package utils

import (
	"maps"
	"regexp"

	"github.com/char2cs/asynx/internal/bus/models"
)

// IsExactPattern reports whether pattern contains no regexp metacharacters,
// i.e. it can be matched with a plain string equality check.
func IsExactPattern(
	pattern string,
) bool {
	return regexp.QuoteMeta(pattern) == pattern
}

// IndexWith returns a new PublishIndex with sub added. The caller must hold subMu.
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
		newRegex := make([]*models.Subscription[T], len(old.RegexSubs)+1)
		copy(newRegex, old.RegexSubs)
		newRegex[len(old.RegexSubs)] = sub
		return &models.PublishIndex[T]{ExactSubs: old.ExactSubs, RegexSubs: newRegex}
	}

	existing := old.ExactSubs[sub.Pattern]
	newSlice := make([]*models.Subscription[T], len(existing)+1)
	copy(newSlice, existing)
	newSlice[len(existing)] = sub

	newExact := make(map[string][]*models.Subscription[T], len(old.ExactSubs)+1)
	maps.Copy(newExact, old.ExactSubs)
	newExact[sub.Pattern] = newSlice

	return &models.PublishIndex[T]{ExactSubs: newExact, RegexSubs: old.RegexSubs}
}

// IndexWithout returns a new PublishIndex with sub removed. The caller must hold subMu.
func IndexWithout[T any](
	old *models.PublishIndex[T],
	sub *models.Subscription[T],
) *models.PublishIndex[T] {
	if !IsExactPattern(sub.Pattern) {
		newRegex := make([]*models.Subscription[T], 0, len(old.RegexSubs)-1)
		for _, s := range old.RegexSubs {
			if s == sub {
				continue
			}
			newRegex = append(newRegex, s)
		}
		return &models.PublishIndex[T]{
			ExactSubs: old.ExactSubs,
			RegexSubs: newRegex,
		}
	}

	existing := old.ExactSubs[sub.Pattern]
	newSlice := make([]*models.Subscription[T], 0, len(existing)-1)
	for _, s := range existing {
		if s == sub {
			continue
		}
		newSlice = append(newSlice, s)
	}

	newExact := make(map[string][]*models.Subscription[T], len(old.ExactSubs))
	maps.Copy(newExact, old.ExactSubs)
	delete(newExact, sub.Pattern)
	if len(newSlice) > 0 {
		newExact[sub.Pattern] = newSlice
	}

	return &models.PublishIndex[T]{
		ExactSubs: newExact,
		RegexSubs: old.RegexSubs,
	}
}

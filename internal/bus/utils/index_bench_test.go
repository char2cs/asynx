package utils_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/char2cs/asynx/internal/bus/models"
	"github.com/char2cs/asynx/internal/bus/utils"
	asynxmd "github.com/char2cs/asynx/models"
)

// buildExactIndex returns a PublishIndex pre-populated with n exact-match subs.
func buildExactIndex(n int) *models.PublishIndex[string] {
	idx := emptyIndex[string]()
	for i := range n {
		sub := &models.Subscription[string]{
			Pattern: fmt.Sprintf("Event%d", i),
			Handler: func(_ context.Context, _ asynxmd.Event[string]) {},
		}
		idx = utils.IndexWith(idx, sub)
	}
	return idx
}

// buildRegexIndex returns a PublishIndex pre-populated with n regex subs.
func buildRegexIndex(n int) (*models.PublishIndex[string], []*models.Subscription[string]) {
	idx := emptyIndex[string]()
	subs := make([]*models.Subscription[string], n)
	for i := range n {
		subs[i] = &models.Subscription[string]{
			Pattern: fmt.Sprintf("^Event%d.*", i),
			Handler: func(_ context.Context, _ asynxmd.Event[string]) {},
		}
		idx = utils.IndexWith(idx, subs[i])
	}
	return idx, subs
}

// BenchmarkIsExactPattern measures the regexp.QuoteMeta comparison used to
// classify every pattern at Subscribe time.
func BenchmarkIsExactPattern(b *testing.B) {
	b.Run("exact", func(b *testing.B) {
		for range b.N {
			utils.IsExactPattern("OrderPlaced")
		}
	})
	b.Run("regex", func(b *testing.B) {
		for range b.N {
			utils.IsExactPattern("^Order.*")
		}
	})
}

// BenchmarkIndexWith_Exact measures COW cost of adding one exact-match sub to
// an index of varying size. The maps.Copy inside is O(n), so this exposes how
// Subscribe slows as the index grows.
func BenchmarkIndexWith_Exact(b *testing.B) {
	for _, n := range []int{0, 100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("%d_existing", n), func(b *testing.B) {
			idx := buildExactIndex(n)
			sub := &models.Subscription[string]{
				Pattern: "NewEvent",
				Handler: func(_ context.Context, _ asynxmd.Event[string]) {},
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				utils.IndexWith(idx, sub)
			}
		})
	}
}

// BenchmarkIndexWith_Regex measures COW cost of adding one regex sub. The regex
// path copies a slice (O(n) in regex count) rather than a map.
func BenchmarkIndexWith_Regex(b *testing.B) {
	for _, n := range []int{0, 100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("%d_existing", n), func(b *testing.B) {
			idx, _ := buildRegexIndex(n)
			sub := &models.Subscription[string]{
				Pattern: "^NewEvent.*",
				Handler: func(_ context.Context, _ asynxmd.Event[string]) {},
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				utils.IndexWith(idx, sub)
			}
		})
	}
}

// BenchmarkIndexWithout_Exact measures COW cost of removing one exact-match sub.
func BenchmarkIndexWithout_Exact(b *testing.B) {
	for _, n := range []int{1, 100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("%d_existing", n), func(b *testing.B) {
			idx := buildExactIndex(n)
			// Pick the first sub to remove (needs a pointer into the index).
			target := idx.ExactSubs["Event0"][0]
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				utils.IndexWithout(idx, target)
			}
		})
	}
}

// BenchmarkIndexWithout_Regex measures COW cost of removing one regex sub.
func BenchmarkIndexWithout_Regex(b *testing.B) {
	for _, n := range []int{1, 100, 1_000, 10_000} {
		b.Run(fmt.Sprintf("%d_existing", n), func(b *testing.B) {
			idx, subs := buildRegexIndex(n)
			target := subs[0]
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				utils.IndexWithout(idx, target)
			}
		})
	}
}

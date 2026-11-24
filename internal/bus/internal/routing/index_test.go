package routing_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/char2cs/asynx/v2/internal/bus/internal/routing"
	"github.com/char2cs/asynx/v2/internal/bus/models"
	"github.com/char2cs/asynx/v2/models/event"
)

func emptyIndex[T any]() *models.PublishIndex[T] {
	return &models.PublishIndex[T]{
		ExactSubs: make(map[string][]*models.Subscription[T]),
	}
}

func newSub(
	pattern string,
) *models.Subscription[string] {
	return &models.Subscription[string]{
		Pattern: pattern,
		Handler: func(
			_ context.Context,
			_ event.Event[string],
		) {
		},
	}
}

func TestIsExactPattern_PlainString(
	t *testing.T,
) {
	for _, p := range []string{"hello", "OrderPlaced", "a", "foo-bar_baz"} {
		assertExactPattern(
			t,
			p,
		)
	}
}

func TestIsExactPattern_RegexChars(
	t *testing.T,
) {
	for _, p := range []string{"^hello", "hello.*", "hell[o", "hello.", "Order+"} {
		assertRegexPattern(
			t,
			p,
		)
	}
}

func assertExactPattern(
	t *testing.T,
	pattern string,
) {
	t.Helper()

	if !routing.IsExactPattern(pattern) {
		t.Errorf(
			"IsExactPattern(%q) = false, want true",
			pattern,
		)
	}
}

func assertRegexPattern(
	t *testing.T,
	pattern string,
) {
	t.Helper()

	if routing.IsExactPattern(pattern) {
		t.Errorf(
			"IsExactPattern(%q) = true, want false",
			pattern,
		)
	}
}

func TestIndexWith_ExactNewEntry(
	t *testing.T,
) {
	idx := emptyIndex[string]()
	sub := newSub("OrderPlaced")

	got := routing.IndexWith(
		idx,
		sub,
	)
	if len(got.ExactSubs["OrderPlaced"]) != 1 {
		t.Errorf(
			"expected 1 sub, got %d",
			len(got.ExactSubs["OrderPlaced"]),
		)
	}

	if got.ExactSubs["OrderPlaced"][0] != sub {
		t.Error("sub pointer mismatch")
	}
}

func TestIndexWith_ExactSameKeyAppended(
	t *testing.T,
) {
	sub1 := newSub("OrderPlaced")
	sub2 := newSub("OrderPlaced")

	idx := routing.IndexWith(
		emptyIndex[string](),
		sub1,
	)
	idx = routing.IndexWith(
		idx,
		sub2,
	)

	subs := idx.ExactSubs["OrderPlaced"]
	if len(subs) != 2 {
		t.Fatalf(
			"expected 2 subs, got %d",
			len(subs),
		)
	}

	if subs[0] != sub1 ||
		subs[1] != sub2 {
		t.Error("sub order/pointer mismatch")
	}
}

func TestIndexWith_Regex(
	t *testing.T,
) {
	sub := newSub("^Order.*")
	idx := routing.IndexWith(
		emptyIndex[string](),
		sub,
	)
	if len(idx.RegexSubs) != 1 {
		t.Fatalf(
			"expected 1 regex sub, got %d",
			len(idx.RegexSubs),
		)
	}

	if idx.RegexSubs[0] != sub {
		t.Error("sub pointer mismatch")
	}

	if len(idx.ExactSubs) != 0 {
		t.Error("exact subs should be empty")
	}
}

func TestIndexWith_COW(
	t *testing.T,
) {
	orig := emptyIndex[string]()
	sub := newSub("OrderPlaced")

	_ = routing.IndexWith(
		orig,
		sub,
	)

	if len(orig.ExactSubs) != 0 {
		t.Error("original index was mutated by IndexWith")
	}
}

func TestIndexWithout_ExactOnlySub(
	t *testing.T,
) {
	sub := newSub("OrderPlaced")
	idx := routing.IndexWith(
		emptyIndex[string](),
		sub,
	)

	got := routing.IndexWithout(
		idx,
		sub,
	)

	if _, ok := got.ExactSubs["OrderPlaced"]; ok {
		t.Error("key should have been deleted")
	}
}

func TestIndexWithout_ExactMultipleSubs(
	t *testing.T,
) {
	sub1 := newSub("OrderPlaced")
	sub2 := newSub("OrderPlaced")
	idx := routing.IndexWith(
		emptyIndex[string](),
		sub1,
	)
	idx = routing.IndexWith(
		idx,
		sub2,
	)

	got := routing.IndexWithout(
		idx,
		sub1,
	)

	subs := got.ExactSubs["OrderPlaced"]
	if len(subs) != 1 {
		t.Fatalf(
			"expected 1 sub remaining, got %d",
			len(subs),
		)
	}

	if subs[0] != sub2 {
		t.Error("wrong sub remaining")
	}
}

func TestIndexWithout_Regex(
	t *testing.T,
) {
	sub1 := newSub("^Order.*")
	sub2 := newSub("^Payment.*")
	idx := routing.IndexWith(
		emptyIndex[string](),
		sub1,
	)
	idx = routing.IndexWith(
		idx,
		sub2,
	)

	got := routing.IndexWithout(
		idx,
		sub1,
	)
	if len(got.RegexSubs) != 1 {
		t.Fatalf(
			"expected 1 regex sub, got %d",
			len(got.RegexSubs),
		)
	}

	if got.RegexSubs[0] != sub2 {
		t.Error("wrong regex sub remaining")
	}
}

func TestIndexWithout_COW(
	t *testing.T,
) {
	sub := newSub("OrderPlaced")
	idx := routing.IndexWith(
		emptyIndex[string](),
		sub,
	)

	_ = routing.IndexWithout(
		idx,
		sub,
	)

	if len(idx.ExactSubs["OrderPlaced"]) != 1 {
		t.Error("original index was mutated by IndexWithout")
	}
}

func buildExactIndex(
	n int,
) *models.PublishIndex[string] {
	idx := emptyIndex[string]()
	for i := range n {
		sub := &models.Subscription[string]{
			Pattern: fmt.Sprintf(
				"Event%d",
				i,
			),
			Handler: func(
				_ context.Context,
				_ event.Event[string],
			) {
			},
		}
		idx = routing.IndexWith(
			idx,
			sub,
		)
	}

	return idx
}

func buildRegexIndex(
	n int,
) (*models.PublishIndex[string], []*models.Subscription[string]) {
	idx := emptyIndex[string]()
	subs := make(
		[]*models.Subscription[string],
		n,
	)
	for i := range n {
		subs[i] = &models.Subscription[string]{
			Pattern: fmt.Sprintf(
				"^Event%d.*",
				i,
			),
			Handler: func(
				_ context.Context,
				_ event.Event[string],
			) {
			},
		}
		idx = routing.IndexWith(
			idx,
			subs[i],
		)
	}

	return idx,
		subs
}

func BenchmarkIsExactPattern(
	b *testing.B,
) {
	b.Run(
		"exact",
		benchmarkIsExactPatternExact,
	)
	b.Run(
		"regex",
		benchmarkIsExactPatternRegex,
	)
}

func benchmarkIsExactPatternExact(
	b *testing.B,
) {
	for range b.N {
		routing.IsExactPattern("OrderPlaced")
	}
}

func benchmarkIsExactPatternRegex(
	b *testing.B,
) {
	for range b.N {
		routing.IsExactPattern("^Order.*")
	}
}

func BenchmarkIndexWith_Exact(
	b *testing.B,
) {
	for _, n := range []int{0, 100, 1_000, 10_000} {
		b.Run(
			fmt.Sprintf(
				"%d_existing",
				n,
			),
			indexWithExact(n),
		)
	}
}

func indexWithExact(
	n int,
) func(*testing.B) {
	return func(
		b *testing.B,
	) {
		benchIndexWith(
			b,
			buildExactIndex(n),
			"NewEvent",
		)
	}
}

func benchIndexWith(
	b *testing.B,
	idx *models.PublishIndex[string],
	pattern string,
) {
	sub := newSub(pattern)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		routing.IndexWith(
			idx,
			sub,
		)
	}
}

func BenchmarkIndexWith_Regex(
	b *testing.B,
) {
	for _, n := range []int{0, 100, 1_000, 10_000} {
		b.Run(
			fmt.Sprintf(
				"%d_existing",
				n,
			),
			indexWithRegex(n),
		)
	}
}

func indexWithRegex(
	n int,
) func(*testing.B) {
	return func(
		b *testing.B,
	) {
		idx, _ := buildRegexIndex(n)
		benchIndexWith(
			b,
			idx,
			"^NewEvent.*",
		)
	}
}

func BenchmarkIndexWithout_Exact(
	b *testing.B,
) {
	for _, n := range []int{1, 100, 1_000, 10_000} {
		b.Run(
			fmt.Sprintf(
				"%d_existing",
				n,
			),
			indexWithoutExact(n),
		)
	}
}

func indexWithoutExact(
	n int,
) func(*testing.B) {
	return func(
		b *testing.B,
	) {
		idx := buildExactIndex(n)
		benchIndexWithout(
			b,
			idx,
			idx.ExactSubs["Event0"][0],
		)
	}
}

func benchIndexWithout(
	b *testing.B,
	idx *models.PublishIndex[string],
	target *models.Subscription[string],
) {
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		routing.IndexWithout(
			idx,
			target,
		)
	}
}

func BenchmarkIndexWithout_Regex(
	b *testing.B,
) {
	for _, n := range []int{1, 100, 1_000, 10_000} {
		b.Run(
			fmt.Sprintf(
				"%d_existing",
				n,
			),
			indexWithoutRegex(n),
		)
	}
}

func indexWithoutRegex(
	n int,
) func(*testing.B) {
	return func(
		b *testing.B,
	) {
		idx, subs := buildRegexIndex(n)
		benchIndexWithout(
			b,
			idx,
			subs[0],
		)
	}
}

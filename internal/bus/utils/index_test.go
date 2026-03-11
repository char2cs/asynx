package utils_test

import (
	"context"
	"testing"

	"github.com/char2cs/asynx/internal/bus/models"
	"github.com/char2cs/asynx/internal/bus/utils"
	asynxmd "github.com/char2cs/asynx/models"
)

func emptyIndex[T any]() *models.PublishIndex[T] {
	return &models.PublishIndex[T]{
		ExactSubs: make(map[string][]*models.Subscription[T]),
	}
}

func newSub(pattern string) *models.Subscription[string] {
	return &models.Subscription[string]{
		Pattern: pattern,
		Handler: func(_ context.Context, _ asynxmd.Event[string]) {},
	}
}

// --- IsExactPattern ---

// 1. Plain string → true.
func TestIsExactPattern_PlainString(t *testing.T) {
	for _, p := range []string{"hello", "OrderPlaced", "a", "foo-bar_baz"} {
		if !utils.IsExactPattern(p) {
			t.Errorf("IsExactPattern(%q) = false, want true", p)
		}
	}
}

// 2. Regex metacharacters → false.
func TestIsExactPattern_RegexChars(t *testing.T) {
	for _, p := range []string{"^hello", "hello.*", "hell[o", "hello.", "Order+"} {
		if utils.IsExactPattern(p) {
			t.Errorf("IsExactPattern(%q) = true, want false", p)
		}
	}
}

// --- IndexWith ---

// 3. Exact pattern, empty index → creates new entry.
func TestIndexWith_ExactNewEntry(t *testing.T) {
	idx := emptyIndex[string]()
	sub := newSub("OrderPlaced")

	got := utils.IndexWith(idx, sub)

	if len(got.ExactSubs["OrderPlaced"]) != 1 {
		t.Errorf("expected 1 sub, got %d", len(got.ExactSubs["OrderPlaced"]))
	}
	if got.ExactSubs["OrderPlaced"][0] != sub {
		t.Error("sub pointer mismatch")
	}
}

// 4. Exact, same key again → appended.
func TestIndexWith_ExactSameKeyAppended(t *testing.T) {
	sub1 := newSub("OrderPlaced")
	sub2 := newSub("OrderPlaced")

	idx := utils.IndexWith(emptyIndex[string](), sub1)
	idx = utils.IndexWith(idx, sub2)

	subs := idx.ExactSubs["OrderPlaced"]
	if len(subs) != 2 {
		t.Fatalf("expected 2 subs, got %d", len(subs))
	}
	if subs[0] != sub1 || subs[1] != sub2 {
		t.Error("sub order/pointer mismatch")
	}
}

// 5. Regex pattern → added to RegexSubs.
func TestIndexWith_Regex(t *testing.T) {
	sub := newSub("^Order.*")
	idx := utils.IndexWith(emptyIndex[string](), sub)

	if len(idx.RegexSubs) != 1 {
		t.Fatalf("expected 1 regex sub, got %d", len(idx.RegexSubs))
	}
	if idx.RegexSubs[0] != sub {
		t.Error("sub pointer mismatch")
	}
	if len(idx.ExactSubs) != 0 {
		t.Error("exact subs should be empty")
	}
}

// 6. COW — original PublishIndex unchanged.
func TestIndexWith_COW(t *testing.T) {
	orig := emptyIndex[string]()
	sub := newSub("OrderPlaced")

	_ = utils.IndexWith(orig, sub)

	if len(orig.ExactSubs) != 0 {
		t.Error("original index was mutated by IndexWith")
	}
}

// --- IndexWithout ---

// 7. Exact, only sub → key deleted.
func TestIndexWithout_ExactOnlySub(t *testing.T) {
	sub := newSub("OrderPlaced")
	idx := utils.IndexWith(emptyIndex[string](), sub)

	got := utils.IndexWithout(idx, sub)

	if _, ok := got.ExactSubs["OrderPlaced"]; ok {
		t.Error("key should have been deleted")
	}
}

// 8. Exact, multiple subs → one removed, others remain.
func TestIndexWithout_ExactMultipleSubs(t *testing.T) {
	sub1 := newSub("OrderPlaced")
	sub2 := newSub("OrderPlaced")
	idx := utils.IndexWith(emptyIndex[string](), sub1)
	idx = utils.IndexWith(idx, sub2)

	got := utils.IndexWithout(idx, sub1)

	subs := got.ExactSubs["OrderPlaced"]
	if len(subs) != 1 {
		t.Fatalf("expected 1 sub remaining, got %d", len(subs))
	}
	if subs[0] != sub2 {
		t.Error("wrong sub remaining")
	}
}

// 9. Regex → removed from RegexSubs.
func TestIndexWithout_Regex(t *testing.T) {
	sub1 := newSub("^Order.*")
	sub2 := newSub("^Payment.*")
	idx := utils.IndexWith(emptyIndex[string](), sub1)
	idx = utils.IndexWith(idx, sub2)

	got := utils.IndexWithout(idx, sub1)

	if len(got.RegexSubs) != 1 {
		t.Fatalf("expected 1 regex sub, got %d", len(got.RegexSubs))
	}
	if got.RegexSubs[0] != sub2 {
		t.Error("wrong regex sub remaining")
	}
}

// 10. COW — original unchanged after IndexWithout.
func TestIndexWithout_COW(t *testing.T) {
	sub := newSub("OrderPlaced")
	idx := utils.IndexWith(emptyIndex[string](), sub)

	_ = utils.IndexWithout(idx, sub)

	if len(idx.ExactSubs["OrderPlaced"]) != 1 {
		t.Error("original index was mutated by IndexWithout")
	}
}

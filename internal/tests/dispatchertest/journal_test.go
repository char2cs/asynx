package dispatchertest

import (
	"slices"
	"testing"
)

func TestJournal_ListsEntriesInTheOrderTheyWereAdded(t *testing.T) {
	var j journal
	j.add(event("a", 1))
	j.note("free text")
	got := j.list()
	got[0] = "changed"
	if want := []string{"a/1", "free text"}; !slices.Equal(j.list(), want) {
		t.Fatalf("list = %v, want %v and a copy", j.list(), want)
	}
}

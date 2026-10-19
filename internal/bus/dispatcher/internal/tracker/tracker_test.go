package tracker

import "testing"

func TestNew_StartsEmpty(t *testing.T) {
	tr := New[string]()
	if _, ok := tr.For("a", 1); ok {
		t.Fatal("For on a new tracker found a ticket")
	}
	tr.Idle()
}

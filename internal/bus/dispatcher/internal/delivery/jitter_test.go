package delivery

import (
	"testing"
	"time"
)

func TestJittered_StaysInTheUpperHalfOfTheDelay(t *testing.T) {
	const delay = 100 * time.Millisecond
	seen := map[time.Duration]struct{}{}
	for range 200 {
		got := jittered(delay)
		if got < delay/2 || got > delay {
			t.Fatalf("jittered(%v) = %v, want within [%v, %v]", delay, got, delay/2, delay)
		}
		seen[got] = struct{}{}
	}
	if len(seen) < 2 {
		t.Fatal("jittered returned the same delay every time")
	}
}

func TestJittered_LeavesTinyDelaysAlone(t *testing.T) {
	for _, delay := range []time.Duration{0, 1, -time.Second} {
		if got := jittered(delay); got != delay {
			t.Fatalf("jittered(%v) = %v, want it unchanged", delay, got)
		}
	}
}

func TestIsClosed_TellsAnOpenChannelFromAClosedOne(t *testing.T) {
	ch := make(chan struct{})
	if isClosed(ch) {
		t.Fatal("an open channel reported closed")
	}
	close(ch)
	if !isClosed(ch) {
		t.Fatal("a closed channel reported open")
	}
}

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
		checkWithinUpperHalf(t, delay, got)
		seen[got] = struct{}{}
	}
	if len(seen) < 2 {
		t.Fatal("jittered returned the same delay every time")
	}
}

func TestJittered_LeavesTinyDelaysAlone(t *testing.T) {
	for _, delay := range []time.Duration{0, 1, -time.Second} {
		checkUnchanged(t, delay, jittered(delay))
	}
}

func checkWithinUpperHalf(
	t *testing.T,
	delay time.Duration,
	got time.Duration,
) {
	t.Helper()
	if got < delay/2 || got > delay {
		t.Fatalf("jittered(%v) = %v, want within [%v, %v]", delay, got, delay/2, delay)
	}
}

func checkUnchanged(
	t *testing.T,
	delay time.Duration,
	got time.Duration,
) {
	t.Helper()
	if got != delay {
		t.Fatalf("jittered(%v) = %v, want it unchanged", delay, got)
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

package asynx

import (
	"testing"
)

func checkExactMatch(
	t *testing.T,
	name string,
	want bool,
) {
	t.Helper()

	got := exactPattern("OrderCreated").matches(name)
	if got != want {
		t.Errorf(
			"matches(%q) = %v, want %v",
			name,
			got,
			want,
		)
	}
}

func TestExactPattern_MatchesOnlyTheSameName(
	t *testing.T,
) {
	checkExactMatch(
		t,
		"OrderCreated",
		true,
	)
	checkExactMatch(
		t,
		"OrderCreated2",
		false,
	)
	checkExactMatch(
		t,
		"ordercreated",
		false,
	)
	checkExactMatch(
		t,
		"",
		false,
	)
}

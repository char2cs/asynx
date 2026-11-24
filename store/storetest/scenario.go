package storetest

import (
	"maps"
	"slices"
	"testing"
)

type scenario[S any] func(
	t testing.TB,
	s S,
)

func runScenarios[S any](
	t *testing.T,
	scenarios map[string]scenario[S],
	newSubject func(*testing.T) S,
) {
	t.Helper()
	for name, fn := range scenarios {
		t.Run(
			name,
			subtestOf(
				fn,
				newSubject,
			),
		)
	}
}

func subtestOf[S any](
	fn scenario[S],
	newSubject func(*testing.T) S,
) func(*testing.T) {
	return func(
		t *testing.T,
	) {
		fn(
			t,
			newSubject(t),
		)
	}
}

func asStrings(
	blobs [][]byte,
) []string {
	out := make(
		[]string,
		len(blobs),
	)
	for i, b := range blobs {
		out[i] = string(b)
	}

	return out
}

func failedOutcome(
	_ string,
	err error,
) bool {
	return err != nil
}

func wantAllFailed(
	t testing.TB,
	outcomes map[string]error,
) {
	t.Helper()
	maps.DeleteFunc(
		outcomes,
		failedOutcome,
	)
	if len(outcomes) != 0 {
		t.Errorf(
			"succeeded on a cancelled context: %v",
			slices.Sorted(maps.Keys(outcomes)),
		)
	}
}

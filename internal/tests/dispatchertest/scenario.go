package dispatchertest

import "testing"

type scenario func(
	t testing.TB,
	h harness,
)

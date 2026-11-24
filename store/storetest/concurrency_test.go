package storetest

import (
	"testing"
)

func TestConcurrencyScenarios_PassOnTheMemoryStore(
	t *testing.T,
) {
	runOnMemoryStore(
		t,
		concurrencyScenarios(),
	)
}

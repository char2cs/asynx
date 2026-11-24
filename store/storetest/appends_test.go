package storetest

import (
	"testing"
)

func TestAppendScenarios_PassOnTheMemoryStore(
	t *testing.T,
) {
	runOnMemoryStore(
		t,
		appendScenarios(),
	)
}

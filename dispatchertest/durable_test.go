package dispatchertest

import "testing"

func TestDurableScenarios_PassOnTheDefault(t *testing.T) {
	runGroup(t, durableScenarios())
}

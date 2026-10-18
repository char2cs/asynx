package dispatchertest

import "testing"

func TestOrderingScenarios_PassOnTheDefault(t *testing.T) {
	runGroup(t, orderingScenarios())
}

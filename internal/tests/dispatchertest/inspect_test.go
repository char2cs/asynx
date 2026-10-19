package dispatchertest

import "testing"

func TestInspectScenarios_PassOnTheDefault(t *testing.T) {
	runGroup(t, inspectScenarios())
}

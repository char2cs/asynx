package dispatchertest

import "testing"

func TestLifecycleScenarios_PassOnTheDefault(t *testing.T) {
	runGroup(t, lifecycleScenarios())
}

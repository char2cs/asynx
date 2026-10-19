package dispatchertest

import "testing"

func TestParkingScenarios_PassOnTheDefault(t *testing.T) {
	runGroup(t, parkingScenarios())
}

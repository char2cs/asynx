package dispatchertest

import "testing"

func TestTicketScenarios_PassOnTheDefault(t *testing.T) {
	runGroup(t, ticketScenarios())
}

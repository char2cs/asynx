package storetest

import (
	"testing"
)

func TestTransactionScenarios_PassOnTheMemoryStore(
	t *testing.T,
) {
	runOnMemoryStore(
		t,
		transactionScenarios(),
	)
}

package storetest

import (
	"testing"
)

func TestDeliveryScenarios_PassOnTheMemoryStore(
	t *testing.T,
) {
	runOnMemoryStore(
		t,
		deliveryScenarios(),
	)
}

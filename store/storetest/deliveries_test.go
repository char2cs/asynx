package storetest

import (
	"testing"

	"github.com/char2cs/asynx/store"
)

func TestDeliveryScenarios_PassOnTheMemoryStore(
	t *testing.T,
) {
	for name, scenario := range deliveryScenarios() {
		t.Run(
			name,
			func(
				t *testing.T,
			) {
				scenario(
					t,
					store.New(),
				)
			},
		)
	}
}

package storetest

import (
	"testing"

	"github.com/char2cs/asynx/store"
)

func TestTransactionScenarios_PassOnTheMemoryStore(
	t *testing.T,
) {
	for name, scenario := range transactionScenarios() {
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

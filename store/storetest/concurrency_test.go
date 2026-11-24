package storetest

import (
	"testing"

	"github.com/char2cs/asynx/store"
)

func TestConcurrencyScenarios_PassOnTheMemoryStore(
	t *testing.T,
) {
	for name, scenario := range concurrencyScenarios() {
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

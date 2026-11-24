package storetest

import (
	"testing"

	"github.com/char2cs/asynx/store"
)

func TestAppendScenarios_PassOnTheMemoryStore(
	t *testing.T,
) {
	for name, scenario := range appendScenarios() {
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

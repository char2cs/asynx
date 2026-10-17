package storetest_test

import (
	"testing"

	"github.com/char2cs/asynx/models"
	"github.com/char2cs/asynx/store"
	"github.com/char2cs/asynx/store/storetest"
)

func TestRun_PassesOnTheMemoryStore(t *testing.T) {
	storetest.Run(t, func(*testing.T) models.Store { return store.New() })
}

package workers_test

import (
	asynxmd "github.com/char2cs/asynx/models"

	"github.com/char2cs/asynx/internal/bus/dispatcher"
)

type testConfig[T any] struct {
	cfg  asynxmd.DispatcherConfig[T]
	opts []dispatcher.Opt[T]
}

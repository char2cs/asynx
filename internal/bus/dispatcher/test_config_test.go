package dispatcher

import (
	asynxmd "github.com/char2cs/asynx/models"
)

type testConfig[T any] struct {
	cfg  asynxmd.DispatcherConfig[T]
	opts []Opt[T]
}

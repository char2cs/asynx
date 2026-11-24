package storetest

import (
	"testing"
)

type scenario[S any] func(
	t testing.TB,
	s S,
)

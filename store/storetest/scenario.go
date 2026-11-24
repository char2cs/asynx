package storetest

import (
	"testing"
)

type scenario[S any] func(
	t testing.TB,
	s S,
)

func asStrings(
	blobs [][]byte,
) []string {
	out := make(
		[]string,
		len(blobs),
	)
	for i, b := range blobs {
		out[i] = string(b)
	}

	return out
}

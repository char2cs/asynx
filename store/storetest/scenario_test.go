package storetest

import (
	"slices"
	"testing"
)

func TestAsStrings_ConvertsEveryBlob(
	t *testing.T,
) {
	got := asStrings([][]byte{[]byte("a"), []byte("bc")})
	if !slices.Equal(
		got,
		[]string{"a", "bc"},
	) {
		t.Fatalf(
			"asStrings = %q, want [a bc]",
			got,
		)
	}
}

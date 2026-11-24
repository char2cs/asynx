package replayer

import "testing"

type boundsCase struct {
	name    string
	window  bounds
	version int64
	want    bool
}

func TestBounds_Contains(
	t *testing.T,
) {
	cases := []boundsCase{
		{"below from", bounds{from: 3, to: 5}, 2, false},
		{"at from", bounds{from: 3, to: 5}, 3, true},
		{"at to", bounds{from: 3, to: 5}, 5, true},
		{"above to", bounds{from: 3, to: 5}, 6, false},
		{"open ended", bounds{from: 3, to: 0}, 1_000, true},
		{"open ended below from", bounds{from: 3, to: 0}, 2, false},
	}
	for _, tc := range cases {
		assertContains(
			t,
			tc,
		)
	}
}

func assertContains(
	t *testing.T,
	tc boundsCase,
) {
	t.Helper()

	got := tc.window.contains(tc.version)
	if got != tc.want {
		t.Errorf(
			"%s: contains(%d) = %v, want %v",
			tc.name,
			tc.version,
			got,
			tc.want,
		)
	}
}

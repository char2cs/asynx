package utils

import "slices"

// FirstError calls try on each item in order and returns the first non-nil
// error, without calling it on the items after that one. It returns nil when
// every call succeeds.
func FirstError[E any](
	items []E,
	try func(E) error,
) error {
	var failure error
	_ = slices.IndexFunc(
		items,
		func(item E) bool {
			failure = try(item)
			return failure != nil
		},
	)
	return failure
}

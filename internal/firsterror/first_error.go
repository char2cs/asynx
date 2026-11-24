package firsterror

// Of calls try on each item in order and returns the first non-nil
// error, without calling it on the items after that one. It returns nil when
// every call succeeds.
func Of[E any](
	items []E,
	try func(E) error,
) error {
	if len(items) == 0 {
		return nil
	}

	err := try(items[0])
	if err != nil {
		return err
	}

	return Of(
		items[1:],
		try,
	)
}

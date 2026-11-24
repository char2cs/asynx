package firsterror

// Of calls try on each item in order and returns the first non-nil error,
// without calling it on the items after that one. It returns nil when every
// call succeeds or items is empty. The error is returned as try produced it,
// not wrapped, so errors.Is and errors.As see through it. Of runs on the
// calling goroutine and is safe for concurrent use if try is.
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

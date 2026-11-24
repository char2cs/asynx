package replayer

type bounds struct {
	from int64
	to   int64
}

func (b bounds) contains(
	version int64,
) bool {
	return version >= b.from &&
		(b.to == 0 ||
			version <= b.to)
}

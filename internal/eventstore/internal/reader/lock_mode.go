package reader

type lockMode int

const (
	lockNotHeld lockMode = iota
	lockHeld
)

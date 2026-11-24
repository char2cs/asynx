package contracts

// Registry holds one record per aggregate under one mutex. Each method is
// atomic: it takes the mutex once and changes the record entirely under that
// hold.
type Registry[J any] interface {
	Backlog[J]
	Parking[J]
	Winddown
}

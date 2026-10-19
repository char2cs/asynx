package delivery

type report struct {
	kind   string
	err    error
	parked []string
}

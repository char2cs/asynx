package asynx

type exactPattern string

func (p exactPattern) matches(
	name string,
) bool {
	return name == string(p)
}

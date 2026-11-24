package mocks

// Order is the aggregate the mock commands operate on.
type Order struct {
	ID     string
	Total  float64
	Status string
}

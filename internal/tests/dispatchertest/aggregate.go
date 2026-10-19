package dispatchertest

// Aggregate is the aggregate state the suite's events carry: the dispatchers
// under test are models.Dispatcher[Aggregate], built by the factory given to
// Run. The suite never inspects it; it exists so that an implementation is
// exercised through a concrete instantiation of its generic type.
type Aggregate struct {
	Name string
}

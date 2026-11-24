package aggregates

// New returns an empty Registry. The hooks run under the registry mutex, when
// Enqueue queues a job (Admit); it may be nil.
func New[J Item](
	hooks Hooks[J],
) Registry[J] {
	return &aggregateRegistry[J]{
		records: make(map[string]*record[J]),
		hooks:   hooks,
	}
}

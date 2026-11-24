package aggregates

// New returns an empty Registry. The hooks run under the registry mutex, when
// Enqueue gives an aggregate a worker (Spawn) and when it queues a job (Admit);
// either may be nil.
func New[J Item](
	hooks Hooks[J],
) Registry[J] {
	return &aggregateRegistry[J]{
		records: make(map[string]*record[J]),
		hooks:   hooks,
	}
}

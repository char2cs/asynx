package aggregates

import "github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/aggregates/contracts"

// Registry holds one record per aggregate under one mutex. The lock rule, the
// park state table and the contract of each method are documented on
// contracts.Registry.
type Registry[J Item] = contracts.Registry[J]

package aggregates

import "github.com/char2cs/asynx/internal/processor/internal/dispatcher/internal/aggregates/contracts"

// Registry holds one record per aggregate under one mutex (see the package
// doc for the lock rule and the park state table). See contracts.Registry for
// the contract of each method.
type Registry[J Item] = contracts.Registry[J]

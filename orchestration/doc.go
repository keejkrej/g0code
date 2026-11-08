// Package orchestration implements T3 Code's orchestration v2 domain model.
//
// App-owned IDs identify durable entities. Provider-native IDs are refs used
// only for correlation and adapter routing. Commands produce an atomic event
// batch and receipt; projections are deterministic folds of those events.
// Provider effects are returned only after commit, keeping process I/O outside
// the domain transaction.
//
// This package is a port of orchestration v2 at t3code v0.1.7. It intentionally
// has no dependency on MyGo, a provider SDK, or a particular database.
package orchestration

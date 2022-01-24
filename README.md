# bitempura ⌛... ⏳!

[![Go Reference](https://pkg.go.dev/badge/github.com/elh/bitempura.svg)](https://pkg.go.dev/github.com/elh/bitempura)
[![Build Status](https://github.com/elh/bitempura/actions/workflows/go.yml/badge.svg?branch=main)](https://github.com/elh/bitempura/actions/workflows/go.yml?query=branch%3Amain)

**Bitempura.DB is a [bitemporal](https://en.wikipedia.org/wiki/Bitemporal_Modeling) key-value database with an [in-memory reference implementation](https://github.com/elh/bitempura/blob/main/memory/db.go).**

### Bitemporality

Temporal databases model time as a core aspect of storing and querying data. A bitemporal database is one that supports these orthogonal axes.
* **Valid time**: When the fact was *true* in the real world. This is the *application domain's* notion of time.
* **Transaction time**: When the fact was *recorded* in the database. This is the *system's* notion of time.

Because every fact in a bitemporal database has these two dimensions, it enables use cases like...
```go
// What was Bob's balance on Jan 1 as best we knew on Jan 8? (VT = Jan 1, TT = Jan 8)
doc, err := db.Find("Bob/balance", AsOfValidTime(jan1), AsOfTransactionTime(jan8))

// But what was it on Jan 1 as best we now know? (VT = Jan 1, TT = now)
doc2, err := db.Find("Bob/balance", AsOfValidTime(jan1))

// We just learned that Bob had a temporary charge from Dec 30 to Jan 3. (VT start = Dec 30, VT end = Jan 3)
// Retroactively add it.
err := db.Put("Bob/balance", Attributes{"dollars": 90}, WithValidTime(dec30), WithEndValidTime(jan3))

// And let's double check all of our transactions and known states.
versions, err := db.History("Bob/balance")
```
*See [full exampes](https://github.com/elh/bitempura/blob/main/memory/db_examples_test.go)

Using a bitemporal database allows you to offload management of temporal application data (valid time) and data versions (transaction time) from your code and onto infrastructure. This provides a universal "time travel" capability across models in the database. Adopting these capabilities proactively is valuable because by the time you realize you need to update (or have already updated) data, it may be too late. Context may already be lost or painful to reconstruct manually.

### Design

* Initial DB API is inspired by XTDB (and Datomic).
* Record layout is inspired by Snodgrass' SQL implementations.

```go
// DB for bitemporal data.
//
// Temporal control options
// On writes: WithValidTime, WithEndValidTime
// On reads: AsOfValidTime, AsOfTransactionTime
type DB interface {
	// Find data by id (as of optional valid and transaction times).
	Find(id string, opts ...ReadOpt) (*Document, error)
	// List all data (as of optional valid and transaction times).
	List(opts ...ReadOpt) ([]*Document, error)
	// Put stores attributes (with optional start and end valid time).
	Put(id string, attributes Attributes, opts ...WriteOpt) error
	// Delete removes attributes (with optional start and end valid time).
	Delete(id string, opts ...WriteOpt) error

	// History returns versions by descending end transaction time, descending end valid time
	History(id string) ([]*Document, error)
}
```

### Author

I'm learning about [bitemporal databases](https://en.wikipedia.org/wiki/Bitemporal_Modeling) and thought the best way to build intuition about their internal design was by building a simple one for myself. I am primarily interesting in:
* Creating materials to teach others
* Building tools to gracefully extend existing SQL databases with bitemporality

Bitempura was the name of my time travelling shrimp. RIP 2049-2021. 🍤

See [TODO](https://github.com/elh/bitempura/blob/main/TODO.md) for more.

# bitempura ⌛... ⏳!

[![Go Reference](https://pkg.go.dev/badge/github.com/elh/bitempura.svg)](https://pkg.go.dev/github.com/elh/bitempura)
[![Build Status](https://github.com/elh/bitempura/actions/workflows/go.yml/badge.svg?branch=main)](https://github.com/elh/bitempura/actions/workflows/go.yml?query=branch%3Amain)

**Bitempura.DB is a [bitemporal](https://en.wikipedia.org/wiki/Bitemporal_Modeling) key-value database with an [in-memory reference implementation](https://github.com/elh/bitempura/blob/main/memory/db.go).**

<br />

## Bitemporality

Temporal databases model time as a core aspect of storing and querying data. A bitemporal database is one that supports these orthogonal axes.
* **Valid time**: When the fact was *true* in the real world. This is the *application domain's* notion of time.
* **Transaction time**: When the fact was *recorded* in the database. This is the *system's* notion of time.

Because every fact in a bitemporal database has these two dimensions, it enables use cases like this:
```go
// We initialize a DB and start using it like an ordinary key-value store.
db, err := memory.NewDB()
err := db.Set("Bob/balance", Attributes{"dollars": 100})
doc, err := db.Get("Bob/balance")
err := db.Delete("Alice/balance")
// and so on...

// We later learn that Bob had a temporary pending charge we missed from Dec 30 to Jan 3. (VT start = Dec 30, VT end = Jan 3)
// Retroactively record it! This does not change his balance today nor does it destroy any history we had about that period.
err := db.Set("Bob/balance", Attributes{"dollars": 90}, WithValidTime(dec30), WithEndValidTime(jan3))

// We can at any point seamlessly ask questions about the real world past AND database record past!
// "What was Bob's balance on Jan 1 as best we knew on Jan 8?" (VT = Jan 1, TT = Jan 8)
doc, err := db.Get("Bob/balance", AsOfValidTime(jan1), AsOfTransactionTime(jan8))

// More time passes and more corrections are made... When trying to make sense of what happened last month, we can ask again:
// "But what was it on Jan 1 as best we now know?" (VT = Jan 1, TT = now)
doc2, err := db.Get("Bob/balance", AsOfValidTime(jan1))

// And while we are at it, let's double check all of our transactions and known states for Bob's balance.
versions, err := db.History("Bob/balance")
```
*See [full exampes](https://github.com/elh/bitempura/blob/main/memory/db_examples_test.go)

Using a bitemporal database allows you to offload management of temporal application data (valid time) and data versions (transaction time) from your code and onto infrastructure. This provides a universal "time travel" capability across models in the database. Adopting these capabilities proactively is valuable because by the time you realize you need to update (or have already updated) data, it may be too late. Context may already be lost or painful to reconstruct manually.

<br />

## Design

* Initial DB API is inspired by XTDB (and Datomic).
* Record layout is inspired by Snodgrass' SQL implementations.

```go
// DB for bitemporal data.
//
// Temporal control options
// On writes: WithValidTime, WithEndValidTime
// On reads: AsOfValidTime, AsOfTransactionTime
type DB interface {
	// Get data by key (as of optional valid and transaction times).
	Get(key string, opts ...ReadOpt) (*Document, error)
	// List all data (as of optional valid and transaction times).
	List(opts ...ReadOpt) ([]*Document, error)
	// Set stores attributes (with optional start and end valid time).
	Set(key string, attributes Attributes, opts ...WriteOpt) error
	// Delete removes attributes (with optional start and end valid time).
	Delete(key string, opts ...WriteOpt) error

	// History returns versions by descending end transaction time, descending end valid time
	History(key string) ([]*Document, error)
}
```

<br />

## Author

I'm learning about [bitemporal databases](https://en.wikipedia.org/wiki/Bitemporal_Modeling) and thought the best way to build intuition about their internal design was by building a simple one for myself. My goals are:
* Making this a viable, standalone lib
* Creating artifacts to teach others about temporal data
* Launching off this to new tools for gracefully extending existing SQL databases with bitemporality

Bitempura was the name of my time travelling shrimp. RIP 2049-2022. 🦐

See [TODO](https://github.com/elh/bitempura/blob/main/TODO.md) for more.

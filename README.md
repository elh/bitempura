# bitemporal ⌛

Building intuition about [bitemporal databases](https://en.wikipedia.org/wiki/Bitemporal_Modeling) by building one for myself.

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
}
```

See working [in memory reference implementation](https://github.com/elh/bitemporal/blob/main/memory/db.go)

See [TODO](https://github.com/elh/bitemporal/blob/main/TODO.md)

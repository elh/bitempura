package bitemporal

import (
	"time"
)

// DB for bitemporal data.
//
// Temporal control options.
// On writes: WithValidTime, WithEndValidTime.
// On reads: AsOfValidTime, AsOfTransactionTime.
type DB interface {
	// Find document by id as of specified times.
	Find(id string, opts ...ReadOpt) (*Document, error)
	// List all documents as of specified times.
	List(opts ...ReadOpt) ([]*Document, error)
	// Put stores attributes with optional configured valid times.
	Put(id string, attributes Attributes, opts ...WriteOpt) error
	// Delete removes attributes with optional configured valid times.
	Delete(id string, opts ...WriteOpt) error

	// History returns versions by descending end transaction time, descending end valid time
	History(id string) ([]*Document, error)
}

type WriteOptions struct {
	ValidTime    time.Time
	EndValidTime *time.Time
}

// WriteOpt is an option for database writes
type WriteOpt func(*WriteOptions)

// WithValidTime allows writer to configure explicit valid time
func WithValidTime(t time.Time) WriteOpt {
	return func(os *WriteOptions) {
		os.ValidTime = t
	}
}

// WithEndValidTime allows writer to configure explicit end valid time
func WithEndValidTime(t time.Time) WriteOpt {
	return func(os *WriteOptions) {
		os.EndValidTime = &t
	}
}

type ReadOptions struct {
	ValidTime time.Time
	TxTime    time.Time
}

// ReadOpt is an option for database reads
type ReadOpt func(*ReadOptions)

// AsOfValidTime allows reader to read as of a specified valid time
func AsOfValidTime(t time.Time) ReadOpt {
	return func(os *ReadOptions) {
		os.ValidTime = t
	}
}

// AsOfTransactionTime allows reader to read as of a specified transaction time
func AsOfTransactionTime(t time.Time) ReadOpt {
	return func(os *ReadOptions) {
		os.TxTime = t
	}
}

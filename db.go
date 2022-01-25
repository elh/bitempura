package bitempura

import (
	"time"
)

// DB for bitemporal data.
//
// Temporal control options.
// ReadOpt's: AsOfValidTime, AsOfTransactionTime.
// WriteOpt's: WithValidTime, WithEndValidTime.
type DB interface {
	// Get data by key (as of optional valid and transaction times).
	Get(key string, opts ...ReadOpt) (*VersionedKV, error)
	// List all data (as of optional valid and transaction times).
	List(opts ...ReadOpt) ([]*VersionedKV, error)
	// Set stores value (with optional start and end valid time).
	Set(key string, value Value, opts ...WriteOpt) error
	// Delete removes value (with optional start and end valid time).
	Delete(key string, opts ...WriteOpt) error

	// History returns all versioned key-values for key by descending end transaction time, descending end valid time.
	History(key string) ([]*VersionedKV, error)
}

// WriteOptions is a struct for processing WriteOpt's to be used by DB
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

// ReadOptions is a struct for processing ReadOpt's to be used by DB
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

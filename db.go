package bitemporal

import "time"

type Attributes map[string]interface{}

// Document is the core data type
// transaction and valid time starts are inclusive and ends are exclusive
type Document struct {
	ID             string
	TxTimeStart    time.Time
	TxTimeEnd      *time.Time
	ValidTimeStart time.Time
	ValidTimeEnd   *time.Time
	Attributes     Attributes
}

// DB for bitemporal documents
type DB interface {
	Find(id string, opts ...ReadOpt) (*Document, error)
	List(opts ...ReadOpt) ([]*Document, error)
	Put(id string, attributes Attributes, opts ...WriteOpt) error
	Delete(id string, opts ...WriteOpt) error
}

// Temporal controls
// On writes: WithValidTime, WithEndValidTime
// On reads: AsOfValidTime, AsOfTransactionTime

type writeOptions struct {
	validTime    time.Time
	endValidTime time.Time
}

type WriteOpt func(*writeOptions)

func WithValidTime(t time.Time) WriteOpt {
	return func(os *writeOptions) {
		os.validTime = t
	}
}

func WithEndValidTime(t time.Time) WriteOpt {
	return func(os *writeOptions) {
		os.endValidTime = t
	}
}

type readOptions struct {
	validTime       time.Time
	transactionTime time.Time
}

type ReadOpt func(*readOptions)

func AsOfValidTime(t time.Time) ReadOpt {
	return func(os *readOptions) {
		os.validTime = t
	}
}

func AsOfTransactionTime(t time.Time) ReadOpt {
	return func(os *readOptions) {
		os.transactionTime = t
	}
}

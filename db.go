package bitemporal

import (
	"errors"
	"time"
)

type Attributes map[string]interface{}

// Document is the core data type
// transaction and valid time starts are inclusive and ends are exclusive
// TODO: separate "db" model and "storage" model. things got weird
type Document struct {
	ID             string
	TxTimeStart    time.Time
	TxTimeEnd      *time.Time
	ValidTimeStart time.Time
	ValidTimeEnd   *time.Time
	Attributes     Attributes
}

func (d *Document) Validate() error {
	if d.ID == "" {
		return errors.New("id is required")
	}
	if d.TxTimeStart.IsZero() {
		return errors.New("transaction time start cannot be zero value")
	}
	if d.TxTimeEnd != nil {
		if d.TxTimeEnd.IsZero() {
			return errors.New("transaction time end cannot be zero value")
		}
		if !d.TxTimeStart.Before(*d.TxTimeEnd) {
			return errors.New("transaction time start must be before end")
		}
	}
	if d.ValidTimeStart.IsZero() {
		return errors.New("valid time start cannot be zero value")
	}
	if d.ValidTimeEnd != nil {
		if d.ValidTimeEnd.IsZero() {
			return errors.New("valid time end cannot be zero value")
		}
		if !d.ValidTimeStart.Before(*d.ValidTimeEnd) {
			return errors.New("valid time start must be before end")
		}
	}
	if d.Attributes == nil {
		return errors.New("attributes cannot be null")
	}
	return nil
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
	endValidTime *time.Time
}

type WriteOpt func(*writeOptions)

func WithValidTime(t time.Time) WriteOpt {
	return func(os *writeOptions) {
		os.validTime = t
	}
}

func WithEndValidTime(t time.Time) WriteOpt {
	return func(os *writeOptions) {
		os.endValidTime = &t
	}
}

type readOptions struct {
	validTime time.Time
	txTime    time.Time
}

type ReadOpt func(*readOptions)

func AsOfValidTime(t time.Time) ReadOpt {
	return func(os *readOptions) {
		os.validTime = t
	}
}

func AsOfTransactionTime(t time.Time) ReadOpt {
	return func(os *readOptions) {
		os.txTime = t
	}
}

package bitempura

import (
	"errors"
	"time"
)

// VersionedValue is the core data type. Transaction and valid time starts are inclusive and ends are exclusive
type VersionedValue struct {
	Key   string
	Value Value

	TxTimeStart    time.Time
	TxTimeEnd      *time.Time
	ValidTimeStart time.Time
	ValidTimeEnd   *time.Time
}

// Value is the user-controlled data associated with a key (and valid and transaction time information) in the database.
type Value interface{}

// Validate a versioned value
func (d *VersionedValue) Validate() error {
	if d.Key == "" {
		return errors.New("key is required")
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
	return nil
}

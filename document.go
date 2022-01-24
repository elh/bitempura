package bitempura

import (
	"errors"
	"time"
)

// Document is the core data type. Transaction and valid time starts are inclusive and ends are exclusive
type Document struct {
	// TODO(elh): Separate "db" model and "storage" model. The breakdown of "documents" and assignment of tx times is
	// an internal detail that is implementation specific
	ID             string
	TxTimeStart    time.Time
	TxTimeEnd      *time.Time
	ValidTimeStart time.Time
	ValidTimeEnd   *time.Time
	Attributes     Attributes
}

// Attributes is the user-controlled data tracked by the database.
type Attributes map[string]interface{}

// Validate a document
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

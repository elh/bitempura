package bitemporal

import (
	"errors"
	"fmt"
	"time"
)

var ErrNotFound = errors.New("not found")

// NewMemoryDB constructs a in-memory bitemporal DB
// it may optionally be seeded with documents
func NewMemoryDB(documents map[string][]*Document) *memoryDB {
	if documents == nil {
		documents = map[string][]*Document{}
	}
	return &memoryDB{documents: documents}
}

type memoryDB struct {
	now       *time.Time
	documents map[string][]*Document // id -> all "versions" of the document
}

func (db *memoryDB) Find(id string, opts ...ReadOpt) (*Document, error) {
	options := db.handleReadOpts(opts)

	vs, ok := db.documents[id]
	if !ok {
		return nil, ErrNotFound
	}
	return db.findVersionByTime(vs, options.validTime, options.txTime)
}

func (db *memoryDB) List(opts ...ReadOpt) ([]*Document, error) {
	return nil, errors.New("unimplemented")
}

func (db *memoryDB) Put(id string, attributes Attributes, opts ...WriteOpt) error {
	return errors.New("unimplemented")
}

func (db *memoryDB) Delete(id string, opts ...WriteOpt) error {
	return errors.New("unimplemented")
}

//nolint:unused,deadcode // writes unimplemented
func (db *memoryDB) handleWriteOpts(opts []WriteOpt) *writeOptions {
	now := db.getNow()
	options := &writeOptions{
		validTime:    now,
		endValidTime: nil,
	}
	for _, opt := range opts {
		opt(options)
	}
	return options
}

func (db *memoryDB) handleReadOpts(opts []ReadOpt) *readOptions {
	now := db.getNow()
	options := &readOptions{
		validTime: now,
		txTime:    now,
	}
	for _, opt := range opts {
		opt(options)
	}
	return options
}

// handle time properties

// if no match, return ErrNotFound
// if more than 1 possible match, return error
func (db *memoryDB) findVersionByTime(vs []*Document, validTime, txTime time.Time) (*Document, error) {
	var out *Document
	for _, v := range vs {
		if db.inRange(validTime, v.ValidTimeStart, v.ValidTimeEnd) &&
			db.inRange(txTime, v.TxTimeStart, v.TxTimeEnd) {
			if out != nil {
				return nil, fmt.Errorf("multiple versions matched find for validTime: %v, txTime: %v", validTime, txTime)
			}
			out = v
		}
	}
	if out == nil {
		return nil, ErrNotFound
	}
	return out, nil
}

func (db *memoryDB) inRange(t, start time.Time, end *time.Time) bool {
	return (t.Equal(start) || t.After(start)) &&
		(end == nil || t.Before(*end))
}

// for testing

// SetNow overrides "now" used by the DB for transaction times. By default, memoryDB uses time.Now()
// for transaction times. If SetNow used, "now" must be handled manually for all future uses of this db.
func (db *memoryDB) SetNow(t time.Time) {
	db.now = &t
}

func (db *memoryDB) getNow() time.Time {
	if db.now != nil {
		return *db.now
	}
	return time.Now()
}

// when doing a new write, ensure that "now" is monotonically increasing for all transaction times in db.
//nolint:unused,deadcode // writes unimplemented
func (db *memoryDB) validateNow() error {
	var latestInDB time.Time
	for _, versions := range db.documents {
		for _, v := range versions {
			if v.TxTimeStart.After(latestInDB) {
				latestInDB = v.TxTimeStart
			}
			if v.TxTimeEnd.After(latestInDB) {
				latestInDB = *v.TxTimeEnd
			}
		}
	}
	now := db.getNow()
	if !now.After(latestInDB) {
		return fmt.Errorf("now (%v) is not later that last transaction time in db (%v)", now, latestInDB)
	}
	return nil
}

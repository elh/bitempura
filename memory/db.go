package memory

import (
	"errors"
	"fmt"
	"time"

	. "github.com/elh/bitemporal"
)

var _ DB = (*db)(nil)

// NewDB constructs a in-memory bitemporal DB.
// It may optionally be seeded with documents and transaction time may be controlled with SetNow.
func NewDB(documents map[string][]*Document) *db {
	if documents == nil {
		documents = map[string][]*Document{}
	}
	return &db{documents: documents}
}

type db struct {
	now       *time.Time
	documents map[string][]*Document // id -> all "versions" of the document
}

// Find data by id (as of optional valid and transaction times).
func (db *db) Find(id string, opts ...ReadOpt) (*Document, error) {
	if id == "" {
		return nil, ErrIDRequired
	}
	options := db.handleReadOpts(opts)

	vs, ok := db.documents[id]
	if !ok {
		return nil, ErrNotFound
	}
	return db.findVersionByTime(vs, options.ValidTime, options.TxTime)
}

// List all data (as of optional valid and transaction times).
func (db *db) List(opts ...ReadOpt) ([]*Document, error) {
	options := db.handleReadOpts(opts)

	var ret []*Document
	for _, vs := range db.documents {
		v, err := db.findVersionByTime(vs, options.ValidTime, options.TxTime)
		if errors.Is(err, ErrNotFound) {
			continue
		} else if err != nil {
			return nil, err
		}
		ret = append(ret, v)
	}
	return ret, nil
}

// Put stores attributes (with optional start and end valid time).
func (db *db) Put(id string, attributes Attributes, opts ...WriteOpt) error {
	if id == "" {
		return ErrIDRequired
	}
	return db.updateRecords(id, attributes, opts...)
}

// Delete removes attributes (with optional start and end valid time).
func (db *db) Delete(id string, opts ...WriteOpt) error {
	if id == "" {
		return ErrIDRequired
	}
	return db.updateRecords(id, nil, opts...)
}

// common logic of Put and Delete. handling of existing records and "overhand" is the same. If newAttributes is nil,
// none is created (Delete case).
func (db *db) updateRecords(id string, newAttributes Attributes, opts ...WriteOpt) error {
	options, now, err := db.handleWriteOpts(opts)
	if err != nil {
		return err
	}

	vs, ok := db.documents[id]
	if ok {
		overlappingVs, err := db.findOverlappingValidTimeVersions(vs, options.ValidTime, options.EndValidTime, now)
		if err != nil {
			return err
		}

		for _, overlappingV := range overlappingVs {
			// NOTE(elh): playing fast and loose with just mutating document by ptr
			overlappingV.document.TxTimeEnd = &now

			for _, overhang := range overlappingV.overhangs {
				overhangDoc := &Document{
					ID:             id,
					TxTimeStart:    now,
					TxTimeEnd:      nil,
					ValidTimeStart: overhang.start,
					ValidTimeEnd:   overhang.end,
					Attributes:     overlappingV.document.Attributes,
				}
				if err := overhangDoc.Validate(); err != nil {
					return err
				}
				db.documents[id] = append(db.documents[id], overhangDoc)
			}
		}
	}

	// add newAttributes for Put API, nop for Delete API
	if newAttributes != nil {
		newDoc := &Document{
			ID:             id,
			TxTimeStart:    now,
			TxTimeEnd:      nil,
			ValidTimeStart: options.ValidTime,
			ValidTimeEnd:   options.EndValidTime,
			Attributes:     newAttributes,
		}
		if err := newDoc.Validate(); err != nil {
			return err
		}
		db.documents[id] = append(db.documents[id], newDoc)
	}

	return nil
}

func (db *db) handleWriteOpts(opts []WriteOpt) (options *WriteOptions, now time.Time, err error) {
	// gut check to prevent invalid tx times due to testing overrides
	if err := db.validateNow(); err != nil {
		return nil, time.Time{}, err
	}

	now = db.getNow()
	options = &WriteOptions{
		ValidTime:    now,
		EndValidTime: nil,
	}
	for _, opt := range opts {
		opt(options)
	}

	// validate write option times. this is relevant for Delete even if Put is validated at resource level
	if options.EndValidTime != nil && !options.EndValidTime.After(options.ValidTime) {
		return nil, time.Time{}, errors.New("valid time start must be before end")
	}

	return options, now, nil
}

func (db *db) handleReadOpts(opts []ReadOpt) *ReadOptions {
	now := db.getNow()
	options := &ReadOptions{
		ValidTime: now,
		TxTime:    now,
	}
	for _, opt := range opts {
		opt(options)
	}

	return options
}

// handle time properties

// if no match, return ErrNotFound
// if more than 1 possible match, return error
func (db *db) findVersionByTime(vs []*Document, validTime, txTime time.Time) (*Document, error) {
	var out *Document
	for _, v := range vs {
		if db.isInRange(validTime, timeRange{v.ValidTimeStart, v.ValidTimeEnd}) &&
			db.isInRange(txTime, timeRange{v.TxTimeStart, v.TxTimeEnd}) {
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

type overlappingVersion struct {
	document  *Document
	overhangs []timeRange
}

func (db *db) findOverlappingValidTimeVersions(vs []*Document, validTimeStart time.Time, validTimeEnd *time.Time, txTime time.Time) ([]overlappingVersion, error) {
	var out []overlappingVersion
	for _, v := range vs {
		if !db.isInRange(txTime, timeRange{v.TxTimeStart, v.TxTimeEnd}) {
			continue
		}
		hasOverlap, curOverhang := db.hasOverlap(timeRange{validTimeStart, validTimeEnd}, timeRange{v.ValidTimeStart, v.ValidTimeEnd})
		if !hasOverlap {
			continue
		}
		out = append(out, overlappingVersion{
			document:  v,
			overhangs: curOverhang,
		})
	}

	return out, nil
}

// start is inclusive, end is exclusive
type timeRange struct {
	start time.Time
	end   *time.Time
}

func (db *db) isInRange(t time.Time, r timeRange) bool {
	return (t.Equal(r.start) || t.After(r.start)) && (r.end == nil || t.Before(*r.end))
}

// given 2 time ranges, hasOverlap = true if the two ranges intersect.
// if they overlap, yOverhangs represents that intervals within y that are not in x.
// hasOverlap(a, b) =/= hasOverlap(b, a)
// examples:
//     hasOverlap(|10,20|, |5,50|) -> yOverhangs: [|5,10|, |20,50|]
//     hasOverlap(|10,20|, |15,30|) -> yOverhangs: [|20,30|]
//     hasOverlap(|10,20|, |15,20|) -> yOverhangs: []
//     hasOverlap(|10,20|, |12,13|) -> yOverhangs: []
func (db *db) hasOverlap(x, y timeRange) (hasOverlap bool, yOverhangs []timeRange) {
	hasOverlap = (y.end == nil || x.start.Before(*y.end)) && (x.end == nil || y.start.Before(*x.end))
	if hasOverlap {
		// come up with fancier interval math here
		if y.start.Before(x.start) {
			yOverhangs = append(yOverhangs, timeRange{y.start, &x.start})
		}
		if x.end != nil && (y.end == nil || x.end.Before(*y.end)) {
			yOverhangs = append(yOverhangs, timeRange{*x.end, y.end})
		}
	}

	return hasOverlap, yOverhangs
}

// for testing

// SetNow overrides "now" used by the DB for transaction times. By default, db uses time.Now()
// for transaction times. If SetNow used, "now" must be handled manually for all future uses of this db.
func (db *db) SetNow(t time.Time) {
	db.now = &t
}

func (db *db) getNow() time.Time {
	if db.now != nil {
		return *db.now
	}
	return time.Now()
}

// when doing a new write, ensure that "now" is monotonically increasing for all transaction times in db.
func (db *db) validateNow() error {
	var latestInDB time.Time
	for _, versions := range db.documents {
		for _, v := range versions {
			if v.TxTimeStart.After(latestInDB) {
				latestInDB = v.TxTimeStart
			}
			if v.TxTimeEnd != nil && v.TxTimeEnd.After(latestInDB) {
				latestInDB = *v.TxTimeEnd
			}
		}
	}
	now := db.getNow()
	if now.Before(latestInDB) {
		return fmt.Errorf("now (%v) is before the last transaction time in db (%v)", now, latestInDB)
	}
	return nil
}

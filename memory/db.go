package memory

import (
	"errors"
	"fmt"
	"sort"
	"time"

	bt "github.com/elh/bitempura"
)

var _ bt.DB = (*DB)(nil)

// NewDB constructs a in-memory bitemporal DB.
//
// The database may optionally be seeded with Document "versions". No two Documents for the same id may overlap both
// transaction time and valid time. Transaction times (which normally default to now) may be controlled with SetNow.
func NewDB(documents ...*bt.Document) (*DB, error) {
	db := &DB{documents: map[string][]*bt.Document{}}
	for _, d := range documents {
		if err := d.Validate(); err != nil {
			return nil, err
		}
		if err := db.assertNoOverlap(d, db.documents[d.ID]); err != nil {
			return nil, err
		}
		db.documents[d.ID] = append(db.documents[d.ID], d)
	}
	return db, nil
}

type DB struct {
	now       *time.Time
	documents map[string][]*bt.Document // id -> all "versions" of the document
}

// Find data by id (as of optional valid and transaction times).
func (db *DB) Find(id string, opts ...bt.ReadOpt) (*bt.Document, error) {
	options := db.handleReadOpts(opts)

	vs, ok := db.documents[id]
	if !ok {
		return nil, bt.ErrNotFound
	}
	return db.findVersionByTime(vs, options.ValidTime, options.TxTime)
}

// List all data (as of optional valid and transaction times).
func (db *DB) List(opts ...bt.ReadOpt) ([]*bt.Document, error) {
	options := db.handleReadOpts(opts)

	var ret []*bt.Document
	for _, vs := range db.documents {
		v, err := db.findVersionByTime(vs, options.ValidTime, options.TxTime)
		if errors.Is(err, bt.ErrNotFound) {
			continue
		} else if err != nil {
			return nil, err
		}
		ret = append(ret, v)
	}
	return ret, nil
}

// Put stores attributes (with optional start and end valid time).
func (db *DB) Put(id string, attributes bt.Attributes, opts ...bt.WriteOpt) error {
	return db.updateRecords(id, attributes, opts...)
}

// Delete removes attributes (with optional start and end valid time).
func (db *DB) Delete(id string, opts ...bt.WriteOpt) error {
	return db.updateRecords(id, nil, opts...)
}

// History returns versions by descending end transaction time, descending end valid time
func (db *DB) History(id string) ([]*bt.Document, error) {
	vs, ok := db.documents[id]
	if !ok {
		return nil, bt.ErrNotFound
	}

	out := make([]*bt.Document, len(vs))
	copy(out, vs)
	sort.Slice(out, func(i, j int) bool { // reversed. flip i and j
		return (out[j].TxTimeEnd != nil && out[i].TxTimeEnd != nil && out[j].TxTimeEnd.Before(*out[i].TxTimeEnd)) ||
			(out[j].TxTimeEnd != nil && out[i].TxTimeEnd == nil) ||
			(out[j].TxTimeEnd == out[i].TxTimeEnd &&
				(out[j].ValidTimeEnd != nil && out[i].ValidTimeEnd != nil && out[j].ValidTimeEnd.Before(*out[i].ValidTimeEnd)) ||
				(out[j].ValidTimeEnd != nil && out[i].ValidTimeEnd == nil))
	})
	return out, nil
}

// common logic of Put and Delete. handling of existing records and "overhand" is the same. If newAttributes is nil,
// none is created (Delete case).
func (db *DB) updateRecords(id string, newAttributes bt.Attributes, opts ...bt.WriteOpt) error {
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
				overhangDoc := &bt.Document{
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
				if err := db.assertNoOverlap(overhangDoc, db.documents[id]); err != nil {
					return err
				}
				db.documents[id] = append(db.documents[id], overhangDoc)
			}
		}
	}

	// add newAttributes for Put API, nop for Delete API
	if newAttributes != nil {
		newDoc := &bt.Document{
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
		if err := db.assertNoOverlap(newDoc, db.documents[id]); err != nil {
			return err
		}
		db.documents[id] = append(db.documents[id], newDoc)
	}

	return nil
}

func (db *DB) handleWriteOpts(opts []bt.WriteOpt) (options *bt.WriteOptions, now time.Time, err error) {
	// gut check to prevent invalid tx times due to testing overrides
	if err := db.assertValidNow(); err != nil {
		return nil, time.Time{}, err
	}

	now = db.getNow()
	options = &bt.WriteOptions{
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

func (db *DB) handleReadOpts(opts []bt.ReadOpt) *bt.ReadOptions {
	now := db.getNow()
	options := &bt.ReadOptions{
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
func (db *DB) findVersionByTime(vs []*bt.Document, validTime, txTime time.Time) (*bt.Document, error) {
	var out *bt.Document
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
		return nil, bt.ErrNotFound
	}
	return out, nil
}

type overlappingVersion struct {
	document  *bt.Document
	overhangs []timeRange
}

func (db *DB) findOverlappingValidTimeVersions(vs []*bt.Document, validTimeStart time.Time, validTimeEnd *time.Time, txTime time.Time) ([]overlappingVersion, error) {
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

func (db *DB) isInRange(t time.Time, r timeRange) bool {
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
func (db *DB) hasOverlap(x, y timeRange) (hasOverlap bool, yOverhangs []timeRange) {
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

// when updating version records, ensure we do not create ambiguous overlap
func (db *DB) assertNoOverlap(candidate *bt.Document, xs []*bt.Document) error {
	for _, x := range xs {
		txTimeOverlaps, _ := db.hasOverlap(timeRange{candidate.TxTimeStart, candidate.TxTimeEnd}, timeRange{x.TxTimeStart, x.TxTimeEnd})
		validTimeOverlaps, _ := db.hasOverlap(timeRange{candidate.ValidTimeStart, candidate.ValidTimeEnd}, timeRange{x.ValidTimeStart, x.ValidTimeEnd})
		if txTimeOverlaps && validTimeOverlaps {
			return fmt.Errorf("document versions overlap tx time and valid time")
		}
	}
	return nil
}

// for testing

// SetNow overrides "now" used by the DB for transaction times. By default, DB uses time.Now(). If SetNow is used,
// the DB will stop defaulting to time.Now() for all future uses.
func (db *DB) SetNow(t time.Time) {
	db.now = &t
}

func (db *DB) getNow() time.Time {
	if db.now != nil {
		return *db.now
	}
	return time.Now()
}

// when doing a new write, ensure that "now" is monotonically increasing for all transaction times in db.
func (db *DB) assertValidNow() error {
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

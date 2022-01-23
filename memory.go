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
	options := db.handleReadOpts(opts)

	var ret []*Document
	for _, vs := range db.documents {
		v, err := db.findVersionByTime(vs, options.validTime, options.txTime)
		if errors.Is(err, ErrNotFound) {
			continue
		} else if err != nil {
			return nil, err
		}
		ret = append(ret, v)
	}
	return ret, nil
}

func (db *memoryDB) Put(id string, attributes Attributes, opts ...WriteOpt) error {
	options, now, err := db.handleWriteOpts(opts)
	if err != nil {
		return err
	}

	vs, ok := db.documents[id]
	if ok {
		overlappingVs, err := db.findOverlappingValidTimeVersions(vs, options.validTime, options.endValidTime, now)
		if err != nil {
			return err
		}

		for _, overlappingV := range overlappingVs {
			// note: playing fast and loose with just mutating document by ptr
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

	newDoc := &Document{
		ID:             id,
		TxTimeStart:    now,
		TxTimeEnd:      nil,
		ValidTimeStart: options.validTime,
		ValidTimeEnd:   options.endValidTime,
		Attributes:     attributes,
	}
	if err := newDoc.Validate(); err != nil {
		return err
	}

	db.documents[id] = append(db.documents[id], newDoc)
	return nil
}

func (db *memoryDB) Delete(id string, opts ...WriteOpt) error {
	return errors.New("unimplemented")
}

func (db *memoryDB) handleWriteOpts(opts []WriteOpt) (options *writeOptions, now time.Time, err error) {
	// gut check to prevent invalid tx times due to testing overrides
	if err := db.validateNow(); err != nil {
		return nil, time.Time{}, err
	}

	now = db.getNow()
	options = &writeOptions{
		validTime:    now,
		endValidTime: nil,
	}
	for _, opt := range opts {
		opt(options)
	}
	return options, now, nil
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

func (db *memoryDB) findOverlappingValidTimeVersions(vs []*Document, validTimeStart time.Time, validTimeEnd *time.Time, txTime time.Time) ([]overlappingVersion, error) {
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

func (db *memoryDB) isInRange(t time.Time, r timeRange) bool {
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
func (db *memoryDB) hasOverlap(x, y timeRange) (hasOverlap bool, yOverhangs []timeRange) {
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
func (db *memoryDB) validateNow() error {
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
	if !now.After(latestInDB) {
		return fmt.Errorf("now (%v) is not later that last transaction time in db (%v)", now, latestInDB)
	}
	return nil
}

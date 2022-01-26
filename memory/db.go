package memory

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	bt "github.com/elh/bitempura"
)

var _ bt.DB = (*DB)(nil)

// NewDB constructs a in-memory, bitemporal key-value database.
//
// The database may optionally be seeded with "versioned key-value" records. No two records for the same key may overlap
// both transaction time and valid time. Transaction times (which normally default to now) may optionally be controlled
// with SetNow.
func NewDB(versionedKVs ...*bt.VersionedKV) (*DB, error) {
	db := &DB{vKVs: map[string][]*bt.VersionedKV{}}
	for _, kv := range versionedKVs {
		if err := kv.Validate(); err != nil {
			return nil, err
		}
		if err := db.assertNoOverlap(kv, db.vKVs[kv.Key]); err != nil {
			return nil, err
		}
		db.vKVs[kv.Key] = append(db.vKVs[kv.Key], kv)
	}
	return db, nil
}

// DB is an in-memory, bitemporal key-value database.
type DB struct {
	vKVs map[string][]*bt.VersionedKV // key -> all versioned key-values with the key
	now  *time.Time                   // if manually controlled for testing

	m    sync.RWMutex // synchronize access to vKVs
	nowM sync.RWMutex // synchronize access to now
}

// Get data by key (as of optional valid and transaction times).
func (db *DB) Get(key string, opts ...bt.ReadOpt) (*bt.VersionedKV, error) {
	options := db.handleReadOpts(opts)

	db.m.RLock()
	defer db.m.RUnlock()
	vs, ok := db.vKVs[key]
	if !ok {
		return nil, bt.ErrNotFound
	}
	return db.findVersionByTime(vs, options.ValidTime, options.TxTime)
}

// List all data (as of optional valid and transaction times).
func (db *DB) List(opts ...bt.ReadOpt) ([]*bt.VersionedKV, error) {
	options := db.handleReadOpts(opts)

	var ret []*bt.VersionedKV
	db.m.RLock()
	defer db.m.RUnlock()
	for _, vs := range db.vKVs {
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

// Set stores value (with optional start and end valid time).
func (db *DB) Set(key string, value bt.Value, opts ...bt.WriteOpt) error {
	return db.update(key, value, false, opts...)
}

// Delete removes value (with optional start and end valid time).
func (db *DB) Delete(key string, opts ...bt.WriteOpt) error {
	return db.update(key, nil, true, opts...)
}

// History returns versions by descending end transaction time, descending end valid time
func (db *DB) History(key string) ([]*bt.VersionedKV, error) {
	db.m.RLock()
	defer db.m.RUnlock()
	vs, ok := db.vKVs[key]
	if !ok {
		return nil, bt.ErrNotFound
	}

	out := make([]*bt.VersionedKV, len(vs))
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

// Common logic of Set and Delete. Handling of existing records and "overhand" is the same. If for Delete, do not create
// new VersionedKV.
func (db *DB) update(key string, value bt.Value, isDelete bool, opts ...bt.WriteOpt) error {
	options, now, err := db.handleWriteOpts(opts)
	if err != nil {
		return err
	}

	db.m.Lock()
	defer db.m.Unlock()
	vs, ok := db.vKVs[key]
	if ok {
		overlappingVs, err := db.findOverlappingValidTimeVersions(vs, options.ValidTime, options.EndValidTime, now)
		if err != nil {
			return err
		}

		for _, overlappingV := range overlappingVs {
			// NOTE(elh): playing fast and loose with just mutating versioned value by ptr
			overlappingV.v.TxTimeEnd = &now

			for _, overhang := range overlappingV.overhangs {
				overhangV := &bt.VersionedKV{
					Key:            key,
					Value:          overlappingV.v.Value,
					TxTimeStart:    now,
					TxTimeEnd:      nil,
					ValidTimeStart: overhang.start,
					ValidTimeEnd:   overhang.end,
				}
				if err := overhangV.Validate(); err != nil {
					return err
				}
				if err := db.assertNoOverlap(overhangV, db.vKVs[key]); err != nil {
					return err
				}
				db.vKVs[key] = append(db.vKVs[key], overhangV)
			}
		}
	}

	// add value for Set, add nothing for Delete
	if !isDelete {
		newV := &bt.VersionedKV{
			Key:            key,
			Value:          value,
			TxTimeStart:    now,
			TxTimeEnd:      nil,
			ValidTimeStart: options.ValidTime,
			ValidTimeEnd:   options.EndValidTime,
		}
		if err := newV.Validate(); err != nil {
			return err
		}
		if err := db.assertNoOverlap(newV, db.vKVs[key]); err != nil {
			return err
		}
		db.vKVs[key] = append(db.vKVs[key], newV)
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

	// validate write option times. this is relevant for Delete even if Set is validated at resource level
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
func (db *DB) findVersionByTime(vs []*bt.VersionedKV, validTime, txTime time.Time) (*bt.VersionedKV, error) {
	var out *bt.VersionedKV
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
	v         *bt.VersionedKV
	overhangs []timeRange
}

func (db *DB) findOverlappingValidTimeVersions(vs []*bt.VersionedKV, validTimeStart time.Time, validTimeEnd *time.Time, txTime time.Time) ([]overlappingVersion, error) {
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
			v:         v,
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
func (db *DB) assertNoOverlap(candidate *bt.VersionedKV, xs []*bt.VersionedKV) error {
	for _, x := range xs {
		txTimeOverlaps, _ := db.hasOverlap(timeRange{candidate.TxTimeStart, candidate.TxTimeEnd}, timeRange{x.TxTimeStart, x.TxTimeEnd})
		validTimeOverlaps, _ := db.hasOverlap(timeRange{candidate.ValidTimeStart, candidate.ValidTimeEnd}, timeRange{x.ValidTimeStart, x.ValidTimeEnd})
		if txTimeOverlaps && validTimeOverlaps {
			return fmt.Errorf("versioned values for the same key overlap tx time and valid time")
		}
	}
	return nil
}

// for testing

// SetNow overrides "now" used by the DB for transaction times. By default, DB uses time.Now(). If SetNow is used,
// the DB will stop defaulting to time.Now() for all future uses. This should not be used outside of testing because it
// will corrupt the correctness of transaction times.
func (db *DB) SetNow(t time.Time) {
	db.nowM.Lock()
	defer db.nowM.Unlock()
	db.now = &t
}

func (db *DB) getNow() time.Time {
	db.nowM.RLock()
	defer db.nowM.RUnlock()
	if db.now != nil {
		return *db.now
	}
	return time.Now()
}

// when doing a new write, ensure that "now" is monotonically increasing for all transaction times in db.
func (db *DB) assertValidNow() error {
	var latestInDB time.Time
	db.m.RLock()
	defer db.m.RUnlock()
	for _, versions := range db.vKVs {
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

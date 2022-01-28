package test

import (
	"fmt"
	"sort"
	"testing"
	"time"

	. "github.com/elh/bitempura"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	shortForm = "2006-01-02" // simple time format

	// these test dates are always in the real-world past
	t0 = mustParseTime(shortForm, "2021-12-31")
	t1 = mustParseTime(shortForm, "2022-01-01")
	t2 = mustParseTime(shortForm, "2022-01-02")
	t3 = mustParseTime(shortForm, "2022-01-03")
	t4 = mustParseTime(shortForm, "2022-01-04")
	t5 = mustParseTime(shortForm, "2022-01-05")
)

func mustParseTime(layout, value string) time.Time {
	t, err := time.Parse(layout, value)
	if err != nil {
		panic(err)
	}
	return t
}

// TestGet tests the Get function. dbFn must return a DB under test with the VersionedKV's stored in the database.
func TestGet(t *testing.T, dbFn func(kvs []*VersionedKV) (DB, error)) {
	type fixtures struct {
		name string
		// make sure structs isolated between tests while doing in-mem mutations
		vKVs func() []*VersionedKV
	}

	// 1 initial set
	valuesSingleSet := fixtures{
		name: "single set, no end",
		vKVs: func() []*VersionedKV {
			return []*VersionedKV{
				{
					Key:            "A",
					TxTimeStart:    t1,
					TxTimeEnd:      nil,
					ValidTimeStart: t1,
					ValidTimeEnd:   nil,
					Value:          "Old",
				},
			}
		},
	}
	// 1 initial set with a valid time end
	valuesSingleSetWithEnd := fixtures{
		name: "single set, with end",
		vKVs: func() []*VersionedKV {
			return []*VersionedKV{
				{
					Key:            "A",
					TxTimeStart:    t1,
					TxTimeEnd:      nil,
					ValidTimeStart: t1,
					ValidTimeEnd:   &t3,
					Value:          "Old",
				},
			}
		},
	}
	// // 1 initial set and 1 set with later valid time updating score
	// // this sets a TxTimeEnd for the initial record and creates 2 new ones
	valuesUpdated := fixtures{
		name: "initial set, and then set with later valid time",
		vKVs: func() []*VersionedKV {
			return []*VersionedKV{
				{
					Key:            "A",
					TxTimeStart:    t1,
					TxTimeEnd:      &t3,
					ValidTimeStart: t1,
					ValidTimeEnd:   nil,
					Value:          "Old",
				},
				{
					Key:            "A",
					TxTimeStart:    t3,
					TxTimeEnd:      nil,
					ValidTimeStart: t1,
					ValidTimeEnd:   &t3,
					Value:          "Old",
				},
				{
					Key:            "A",
					TxTimeStart:    t3,
					TxTimeEnd:      nil,
					ValidTimeStart: t3,
					ValidTimeEnd:   nil,
					Value:          "New",
				},
			}
		},
	}
	valuesDeleted := fixtures{
		name: "initial set, and then deletion with later valid time",
		vKVs: func() []*VersionedKV {
			return []*VersionedKV{
				{
					Key:            "A",
					TxTimeStart:    t1,
					TxTimeEnd:      &t3,
					ValidTimeStart: t1,
					ValidTimeEnd:   nil,
					Value:          "Old",
				},
				{
					Key:            "A",
					TxTimeStart:    t3,
					TxTimeEnd:      nil,
					ValidTimeStart: t1,
					ValidTimeEnd:   &t3,
					Value:          "Old",
				},
			}
		},
	}

	type testCase struct {
		desc              string
		key               string
		readOpts          []ReadOpt
		expectErrNotFound bool
		expectErr         bool // this is exclusive of ErrNotFound. this is for unexepcted errors
		expectValue       Value
	}

	testCaseSets := []struct {
		fixtures  fixtures
		testCases []testCase
	}{
		{
			fixtures: fixtures{
				name: "empty db",
				vKVs: func() []*VersionedKV { return nil },
			},
			testCases: []testCase{
				{
					desc:              "not found",
					key:               "A",
					expectErrNotFound: true,
				},
			},
		},
		{
			fixtures: valuesSingleSet,
			testCases: []testCase{
				{
					desc:        "found - default as of times",
					key:         "A",
					expectValue: "Old",
				},
				{
					desc:              "not found - as of valid time T before valid time start",
					key:               "A",
					readOpts:          []ReadOpt{AsOfValidTime(t0)},
					expectErrNotFound: true,
				},
				{
					desc:              "not found - as of tx time T before tx time start",
					key:               "A",
					readOpts:          []ReadOpt{AsOfTransactionTime(t0)},
					expectErrNotFound: true,
				},
				{
					desc:        "found - as of valid time T in range",
					key:         "A",
					readOpts:    []ReadOpt{AsOfValidTime(t2)},
					expectValue: "Old",
				},
				{
					desc:        "found - as of tx time T in range",
					key:         "A",
					readOpts:    []ReadOpt{AsOfTransactionTime(t2)},
					expectValue: "Old",
				},
				{
					desc:        "found - as of valid time T in range (inclusive)",
					key:         "A",
					readOpts:    []ReadOpt{AsOfValidTime(t1)},
					expectValue: "Old",
				},
				{
					desc:        "found - as of tx time T in range (inclusive)",
					key:         "A",
					readOpts:    []ReadOpt{AsOfTransactionTime(t1)},
					expectValue: "Old",
				},
			},
		},
		{
			fixtures: valuesSingleSetWithEnd,
			testCases: []testCase{
				{
					desc:        "found - as of valid and tx time T in range",
					key:         "A",
					readOpts:    []ReadOpt{AsOfValidTime(t2), AsOfTransactionTime(t2)},
					expectValue: "Old",
				},
				// valid time end range
				{
					desc:              "not found - default as of times",
					key:               "A",
					expectErrNotFound: true,
				},
				{
					desc:              "not found - as of valid time T after valid time end",
					key:               "A",
					readOpts:          []ReadOpt{AsOfValidTime(t4)},
					expectErrNotFound: true,
				},
				{
					desc:              "not found - as of valid time T equal to valid time end (exclusive)",
					key:               "A",
					readOpts:          []ReadOpt{AsOfValidTime(t3)},
					expectErrNotFound: true,
				},
			},
		},
		{
			fixtures: valuesUpdated,
			testCases: []testCase{
				{
					desc:        "found - default as of times",
					key:         "A",
					expectValue: "New",
				},
				{
					desc:        "as of tx time now, as of valid time before update",
					key:         "A",
					readOpts:    []ReadOpt{AsOfValidTime(t1)},
					expectValue: "Old",
				},
				{
					desc:        "as of tx time before update, as of valid time now",
					key:         "A",
					readOpts:    []ReadOpt{AsOfTransactionTime(t1)},
					expectValue: "Old",
				},
				{
					desc:        "as of tx time before update, as of valid time before update",
					key:         "A",
					readOpts:    []ReadOpt{AsOfValidTime(t1), AsOfTransactionTime(t1)},
					expectValue: "Old",
				},
			},
		},
		{
			fixtures: valuesDeleted,
			testCases: []testCase{
				{
					desc:              "not found - default as of times",
					key:               "A",
					expectErrNotFound: true,
				},
				{
					desc:        "as of tx time now, as of valid time before update",
					key:         "A",
					readOpts:    []ReadOpt{AsOfValidTime(t1)},
					expectValue: "Old",
				},
				{
					desc:        "as of tx time before update, as of valid time now",
					key:         "A",
					readOpts:    []ReadOpt{AsOfTransactionTime(t1)},
					expectValue: "Old",
				},
				{
					desc:        "as of tx time before update, as of valid time before update",
					key:         "A",
					readOpts:    []ReadOpt{AsOfValidTime(t1), AsOfTransactionTime(t1)},
					expectValue: "Old",
				},
			},
		},
	}
	for _, s := range testCaseSets {
		s := s
		for _, tC := range s.testCases {
			tC := tC
			t.Run(fmt.Sprintf("%v: %v", s.fixtures.name, tC.desc), func(t *testing.T) {
				db, err := dbFn(s.fixtures.vKVs())
				require.Nil(t, err)
				ret, err := db.Get(tC.key, tC.readOpts...)
				if tC.expectErrNotFound {
					require.ErrorIs(t, err, ErrNotFound)
					return
				} else if tC.expectErr {
					require.NotErrorIs(t, err, ErrNotFound)
					require.NotNil(t, err)
					return
				}
				require.Nil(t, err)
				assert.Equal(t, tC.expectValue, ret.Value)
			})
		}
	}
}

// TestList tests the List function. dbFn must return a DB under test with the VersionedKV's stored in the database.
func TestList(t *testing.T, dbFn func(kvs []*VersionedKV) (DB, error)) {
	type fixtures struct {
		name string
		// make sure structs isolated between tests while doing in-mem mutations
		vKVs func() []*VersionedKV
	}

	aValue := &VersionedKV{
		Key:            "A",
		TxTimeStart:    t1,
		TxTimeEnd:      nil,
		ValidTimeStart: t1,
		ValidTimeEnd:   nil,
		Value:          "Old",
	}
	aFixtures := fixtures{
		name: "A values",
		vKVs: func() []*VersionedKV {
			return []*VersionedKV{
				aValue,
			}
		},
	}
	bValue := &VersionedKV{
		Key:            "B",
		TxTimeStart:    t1,
		TxTimeEnd:      &t3,
		ValidTimeStart: t1,
		ValidTimeEnd:   nil,
		Value:          "Old",
	}
	bValueUpdate1 := &VersionedKV{
		Key:            "B",
		TxTimeStart:    t3,
		TxTimeEnd:      nil,
		ValidTimeStart: t1,
		ValidTimeEnd:   &t3,
		Value:          "Old",
	}
	bValueUpdate2 := &VersionedKV{
		Key:            "B",
		TxTimeStart:    t3,
		TxTimeEnd:      nil,
		ValidTimeStart: t3,
		ValidTimeEnd:   nil,
		Value:          "New",
	}
	bFixtures := fixtures{
		name: "A, B values",
		vKVs: func() []*VersionedKV {
			return []*VersionedKV{
				aValue,
				bValue,
				bValueUpdate1,
				bValueUpdate2,
			}
		},
	}

	type testCase struct {
		desc         string
		readOpts     []ReadOpt
		expectErr    bool
		expectValues []*VersionedKV
	}

	testCaseSets := []struct {
		fixtures  fixtures
		testCases []testCase
	}{
		{
			fixtures: fixtures{
				name: "empty db",
				vKVs: func() []*VersionedKV { return nil },
			},
			testCases: []testCase{
				{
					desc:         "not found",
					expectValues: nil,
				},
			},
		},
		{
			fixtures: aFixtures,
			testCases: []testCase{
				{
					desc:         "found - default as of times",
					expectValues: []*VersionedKV{aValue},
				},
			},
		},
		{
			fixtures: bFixtures,
			testCases: []testCase{
				{
					desc:         "found - default as of times",
					expectValues: []*VersionedKV{aValue, bValueUpdate2},
				},
				{
					desc:         "not found - as of transaction time",
					readOpts:     []ReadOpt{AsOfTransactionTime(t0)},
					expectValues: nil,
				},
				{
					desc:         "found - as of valid time",
					readOpts:     []ReadOpt{AsOfValidTime(t2)},
					expectValues: []*VersionedKV{aValue, bValueUpdate1},
				},
			},
		},
	}
	for _, s := range testCaseSets {
		s := s
		for _, tC := range s.testCases {
			tC := tC
			t.Run(fmt.Sprintf("%v: %v", s.fixtures.name, tC.desc), func(t *testing.T) {
				db, err := dbFn(s.fixtures.vKVs())
				require.Nil(t, err)
				ret, err := db.List(tC.readOpts...)
				if tC.expectErr {
					require.NotErrorIs(t, err, ErrNotFound)
					require.NotNil(t, err)
					return
				}
				require.Nil(t, err)
				require.Len(t, ret, len(tC.expectValues))
				if len(tC.expectValues) == 0 {
					return
				}
				assert.Equal(t, sortKVsByKey(tC.expectValues), sortKVsByKey(ret))
			})
		}
	}
}

func sortKVsByKey(ds []*VersionedKV) []*VersionedKV {
	out := make([]*VersionedKV, len(ds))
	copy(out, ds)
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

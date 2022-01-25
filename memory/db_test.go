package memory_test

import (
	"encoding/json"
	"fmt"
	"sort"
	"testing"
	"time"

	. "github.com/elh/bitempura"
	"github.com/elh/bitempura/memory"
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

//nolint:unused,deadcode // debug
func toJSON(v interface{}) string {
	out, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		panic(err)
	}
	return string(out)
}

// values can be any type but I will standardize on "Old", "New", and "Newest" in these tests for legibility

func TestConstructor(t *testing.T) {
	type fixtures struct {
		name string
		// make sure structs isolated between tests while doing in-mem mutations
		vKVs func() []*VersionedKV
	}

	type testCase struct {
		desc      string
		expectErr bool
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
					desc: "okay",
				},
			},
		},
		{
			fixtures: fixtures{
				name: "overlapping transaction time",
				vKVs: func() []*VersionedKV {
					return []*VersionedKV{
						{
							Key:            "A",
							TxTimeStart:    t1,
							TxTimeEnd:      nil,
							ValidTimeStart: t1,
							ValidTimeEnd:   &t2,
							Value:          "Old",
						},
						{
							Key:            "A",
							TxTimeStart:    t2,
							TxTimeEnd:      &t3,
							ValidTimeStart: t2,
							ValidTimeEnd:   nil,
							Value:          "New",
						},
					}
				},
			},
			testCases: []testCase{
				{
					desc: "okay",
				},
			},
		},
		{
			fixtures: fixtures{
				name: "overlapping valid time",
				vKVs: func() []*VersionedKV {
					return []*VersionedKV{
						{
							Key:            "A",
							TxTimeStart:    t1,
							TxTimeEnd:      &t2,
							ValidTimeStart: t2,
							ValidTimeEnd:   &t4,
							Value:          "Old",
						},
						{
							Key:            "A",
							TxTimeStart:    t2,
							TxTimeEnd:      nil,
							ValidTimeStart: t1,
							ValidTimeEnd:   &t3,
							Value:          "New",
						},
					}
				},
			},
			testCases: []testCase{
				{
					desc: "okay",
				},
			},
		},
		{
			fixtures: fixtures{
				name: "overlapping transaction time and valid time",
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
						{
							Key:            "A",
							TxTimeStart:    t2,
							TxTimeEnd:      &t3,
							ValidTimeStart: t2,
							ValidTimeEnd:   nil,
							Value:          "New",
						},
					}
				},
			},
			testCases: []testCase{
				{
					desc:      "returns error",
					expectErr: true,
				},
			},
		},
	}
	for _, s := range testCaseSets {
		s := s
		for _, tC := range s.testCases {
			tC := tC
			t.Run(fmt.Sprintf("%v: %v", s.fixtures.name, tC.desc), func(t *testing.T) {
				_, err := memory.NewDB(s.fixtures.vKVs()...)
				if tC.expectErr {
					require.NotNil(t, err)
					return
				}
				require.Nil(t, err)
			})
		}
	}
}

func TestGet(t *testing.T) {
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
				db, err := memory.NewDB(s.fixtures.vKVs()...)
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

func TestList(t *testing.T) {
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
				db, err := memory.NewDB(s.fixtures.vKVs()...)
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

func TestSet(t *testing.T) {
	type fixtures struct {
		name string
		// make sure structs isolated between tests while doing in-mem mutations
		vKVs func() []*VersionedKV
	}

	// verify writes by checking result of find as of configured valid time and tx time
	type findCheck struct {
		readOpts          []ReadOpt
		expectErrNotFound bool
		expectValue       *VersionedKV
	}

	type testCase struct {
		desc      string
		now       *time.Time // manually control transaction time clock
		key       string
		value     Value
		writeOpts []WriteOpt
		expectErr bool
		// verify writes by checking result of find as of configured valid time and tx time
		findChecks []findCheck
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
					desc:  "basic set",
					now:   &t1,
					key:   "A",
					value: "Old",
					findChecks: []findCheck{
						{
							expectValue: &VersionedKV{
								Key:            "A",
								TxTimeStart:    t1,
								TxTimeEnd:      nil,
								ValidTimeStart: t1,
								ValidTimeEnd:   nil,
								Value:          "Old",
							},
						},
					},
				},
				{
					desc:      "basic set with valid time",
					now:       &t1,
					key:       "A",
					value:     "Old",
					writeOpts: []WriteOpt{WithValidTime(t0)},
					findChecks: []findCheck{
						{
							expectValue: &VersionedKV{
								Key:            "A",
								TxTimeStart:    t1,
								TxTimeEnd:      nil,
								ValidTimeStart: t0,
								ValidTimeEnd:   nil,
								Value:          "Old",
							},
						},
					},
				},
				{
					desc:      "basic set with end valid time",
					now:       &t1,
					key:       "A",
					value:     "Old",
					writeOpts: []WriteOpt{WithEndValidTime(t2)},
					findChecks: []findCheck{
						{
							expectValue: &VersionedKV{
								Key:            "A",
								TxTimeStart:    t1,
								TxTimeEnd:      nil,
								ValidTimeStart: t1,
								ValidTimeEnd:   &t2,
								Value:          "Old",
							},
						},
					},
				},
				{
					desc:      "basic set with valid time and end valid time",
					now:       &t1,
					key:       "A",
					value:     "Old",
					writeOpts: []WriteOpt{WithValidTime(t0), WithEndValidTime(t3)},
					findChecks: []findCheck{
						{
							expectValue: &VersionedKV{
								Key:            "A",
								TxTimeStart:    t1,
								TxTimeEnd:      nil,
								ValidTimeStart: t0,
								ValidTimeEnd:   &t3,
								Value:          "Old",
							},
						},
					},
				},
				{
					desc:  "can set value of nil",
					now:   &t1,
					key:   "A",
					value: nil,
					findChecks: []findCheck{
						{
							expectValue: &VersionedKV{
								Key:            "A",
								TxTimeStart:    t1,
								TxTimeEnd:      nil,
								ValidTimeStart: t1,
								ValidTimeEnd:   nil,
								Value:          nil,
							},
						},
					},
				},
				{
					desc:      "error if key not set",
					now:       &t1,
					key:       "",
					value:     "Old",
					expectErr: true,
				},
				{
					desc:      "error if end valid time before valid time",
					now:       &t1,
					key:       "A",
					value:     "Old",
					writeOpts: []WriteOpt{WithValidTime(t3), WithEndValidTime(t0)},
					expectErr: true,
				},
				{
					desc:      "error if end valid time before valid time (default valid time)",
					now:       &t1,
					key:       "A",
					value:     "Old",
					writeOpts: []WriteOpt{WithEndValidTime(t0)},
					expectErr: true,
				},
				{
					desc:      "error if end valid time equal to valid time",
					now:       &t1,
					key:       "A",
					value:     "Old",
					writeOpts: []WriteOpt{WithValidTime(t0), WithEndValidTime(t0)},
					expectErr: true,
				},
			},
		},
		{
			fixtures: fixtures{
				name: "existing entry - no valid end",
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
			},
			testCases: []testCase{
				{
					desc:  "basic set",
					now:   &t3,
					key:   "A",
					value: "New",
					findChecks: []findCheck{
						{
							expectValue: &VersionedKV{
								Key:            "A",
								TxTimeStart:    t3,
								TxTimeEnd:      nil,
								ValidTimeStart: t3,
								ValidTimeEnd:   nil,
								Value:          "New",
							},
						},
						// before update in valid time
						{
							readOpts: []ReadOpt{AsOfValidTime(t2)},
							expectValue: &VersionedKV{
								Key:            "A",
								TxTimeStart:    t3,
								TxTimeEnd:      nil,
								ValidTimeStart: t1,
								ValidTimeEnd:   &t3,
								Value:          "Old",
							},
						},
						// before update in transaction time
						{
							readOpts: []ReadOpt{AsOfTransactionTime(t2)},
							expectValue: &VersionedKV{
								Key:            "A",
								TxTimeStart:    t1,
								TxTimeEnd:      &t3,
								ValidTimeStart: t1,
								ValidTimeEnd:   nil,
								Value:          "Old",
							},
						},
					},
				},
				{
					desc:      "set w/ valid time end. original record overhands on both sides",
					now:       &t4,
					writeOpts: []WriteOpt{WithValidTime(t2), WithEndValidTime(t3)},
					key:       "A",
					value:     "New",
					findChecks: []findCheck{
						// query as of now for valid time and transaction time. change not visible
						{
							expectValue: &VersionedKV{
								Key:            "A",
								TxTimeStart:    t4,
								TxTimeEnd:      nil,
								ValidTimeStart: t3,
								ValidTimeEnd:   nil,
								Value:          "Old",
							},
						},
						// query as of now for transaction time, before update for valid time. change not visible
						{
							readOpts: []ReadOpt{AsOfValidTime(t1)},
							expectValue: &VersionedKV{
								Key:            "A",
								TxTimeStart:    t4,
								TxTimeEnd:      nil,
								ValidTimeStart: t1,
								ValidTimeEnd:   &t2,
								Value:          "Old",
							},
						},
						// query as of now for valid time, before update for transaction time. change not visible
						{
							readOpts: []ReadOpt{AsOfTransactionTime(t2)},
							expectValue: &VersionedKV{
								Key:            "A",
								TxTimeStart:    t1,
								TxTimeEnd:      &t4,
								ValidTimeStart: t1,
								ValidTimeEnd:   nil,
								Value:          "Old",
							},
						},
						// query as of valid time in range, transaction time after update. change visible
						{
							readOpts: []ReadOpt{AsOfValidTime(t2), AsOfTransactionTime(t5)},
							expectValue: &VersionedKV{
								Key:            "A",
								TxTimeStart:    t4,
								TxTimeEnd:      nil,
								ValidTimeStart: t2,
								ValidTimeEnd:   &t3,
								Value:          "New",
							},
						},
					},
				},
				{
					desc:      "set w/ valid time end. no overhang",
					now:       &t4,
					writeOpts: []WriteOpt{WithValidTime(t1)},
					key:       "A",
					value:     "New",
					findChecks: []findCheck{
						// query as of now for valid time and transaction time. change visible
						{
							expectValue: &VersionedKV{
								Key:            "A",
								TxTimeStart:    t4,
								TxTimeEnd:      nil,
								ValidTimeStart: t1,
								ValidTimeEnd:   nil,
								Value:          "New",
							},
						},
						// query as of now for valid time, before update for transaction time. change not visible
						{
							readOpts: []ReadOpt{AsOfTransactionTime(t2)},
							expectValue: &VersionedKV{
								Key:            "A",
								TxTimeStart:    t1,
								TxTimeEnd:      &t4,
								ValidTimeStart: t1,
								ValidTimeEnd:   nil,
								Value:          "Old",
							},
						},
					},
				},
			},
		},
		{
			fixtures: fixtures{
				name: "existing entries. multiple valid time ranges active",
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
			},
			testCases: []testCase{
				{
					desc:      "set overlaps multiple versions",
					now:       &t4,
					key:       "A",
					writeOpts: []WriteOpt{WithValidTime(t2), WithEndValidTime(t4)},
					value:     "Newest",
					findChecks: []findCheck{
						// TT = t5, VT = t4. after update transaction, not in valid range. too high
						{
							readOpts: []ReadOpt{AsOfTransactionTime(t5)},
							expectValue: &VersionedKV{
								Key:            "A",
								TxTimeStart:    t4,
								TxTimeEnd:      nil,
								ValidTimeStart: t4,
								ValidTimeEnd:   nil,
								Value:          "New",
							},
						},
						// TT = t5, VT = t1. after update transaction, not in valid range. too low
						{
							readOpts: []ReadOpt{AsOfTransactionTime(t5), AsOfValidTime(t1)},
							expectValue: &VersionedKV{
								Key:            "A",
								TxTimeStart:    t4,
								TxTimeEnd:      nil,
								ValidTimeStart: t1,
								ValidTimeEnd:   &t2,
								Value:          "Old",
							},
						},
						// TT = t5, VT = t3. after update transaction, in valid range
						{
							readOpts: []ReadOpt{AsOfTransactionTime(t5), AsOfValidTime(t3)},
							expectValue: &VersionedKV{
								Key:            "A",
								TxTimeStart:    t4,
								TxTimeEnd:      nil,
								ValidTimeStart: t2,
								ValidTimeEnd:   &t4,
								Value:          "Newest",
							},
						},
						// TT = t3, VT = t2 before update transaction, in the fixture original range
						{
							readOpts: []ReadOpt{AsOfTransactionTime(t3), AsOfValidTime(t2)},
							expectValue: &VersionedKV{
								Key:            "A",
								TxTimeStart:    t3,
								TxTimeEnd:      &t4,
								ValidTimeStart: t1,
								ValidTimeEnd:   &t3,
								Value:          "Old",
							},
						},
						// TT = t3, VT = t4. before update transaction, in the fixture updated range
						{
							readOpts: []ReadOpt{AsOfTransactionTime(t3), AsOfValidTime(t4)},
							expectValue: &VersionedKV{
								Key:            "A",
								TxTimeStart:    t3,
								TxTimeEnd:      &t4,
								ValidTimeStart: t3,
								ValidTimeEnd:   nil,
								Value:          "New",
							},
						},
						// TT = t2, VT = t2. before 1st fixture update transaction
						{
							readOpts: []ReadOpt{AsOfTransactionTime(t2), AsOfValidTime(t2)},
							expectValue: &VersionedKV{
								Key:            "A",
								TxTimeStart:    t1,
								TxTimeEnd:      &t3,
								ValidTimeStart: t1,
								ValidTimeEnd:   nil,
								Value:          "Old",
							},
						},
					},
				},
			},
		},
	}
	for _, s := range testCaseSets {
		s := s
		for _, tC := range s.testCases {
			tC := tC
			t.Run(fmt.Sprintf("%v: %v", s.fixtures.name, tC.desc), func(t *testing.T) {
				db, err := memory.NewDB(s.fixtures.vKVs()...)
				require.Nil(t, err)
				if tC.now != nil {
					db.SetNow(*tC.now)
				}
				err = db.Set(tC.key, tC.value, tC.writeOpts...)
				if tC.expectErr {
					require.NotNil(t, err)
					return
				}
				require.Nil(t, err)

				for _, findCheck := range tC.findChecks {
					ret, err := db.Get(tC.key, findCheck.readOpts...)
					if findCheck.expectErrNotFound {
						require.ErrorIs(t, err, ErrNotFound)
						return
					}
					require.Nil(t, err)
					assert.Equal(t, findCheck.expectValue, ret)
				}
			})
		}
	}
}

func TestDelete(t *testing.T) {
	type fixtures struct {
		name string
		// make sure structs isolated between tests while doing in-mem mutations
		vKVs func() []*VersionedKV
	}

	// verify writes by checking result of find as of configured valid time and tx time
	type findCheck struct {
		readOpts          []ReadOpt
		expectErrNotFound bool
		expectValue       *VersionedKV
	}

	type testCase struct {
		desc      string
		now       *time.Time // manually control transaction time clock
		key       string
		writeOpts []WriteOpt
		expectErr bool
		// verify writes by checking result of find as of configured valid time and tx time
		findChecks []findCheck
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
					desc: "delete with no match is nop",
					now:  &t1,
					key:  "A",
					findChecks: []findCheck{
						{
							expectErrNotFound: true,
						},
					},
				},
			},
		},
		{
			fixtures: fixtures{
				name: "existing entry - no valid end",
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
			},
			testCases: []testCase{
				{
					desc:      "error if end valid time before valid time",
					now:       &t2,
					key:       "A",
					writeOpts: []WriteOpt{WithValidTime(t3), WithEndValidTime(t0)},
					expectErr: true,
				},
				{
					desc:      "error if end valid time before valid time (default valid time)",
					now:       &t2,
					key:       "A",
					writeOpts: []WriteOpt{WithEndValidTime(t0)},
					expectErr: true,
				},
				{
					desc:      "error if end valid time equal to valid time",
					now:       &t2,
					key:       "A",
					writeOpts: []WriteOpt{WithValidTime(t0), WithEndValidTime(t0)},
					expectErr: true,
				},
				{
					desc: "basic delete",
					now:  &t3,
					key:  "A",
					findChecks: []findCheck{
						{
							expectErrNotFound: true,
						},
						// before update in valid time
						{
							readOpts: []ReadOpt{AsOfValidTime(t2)},
							expectValue: &VersionedKV{
								Key:            "A",
								TxTimeStart:    t3,
								TxTimeEnd:      nil,
								ValidTimeStart: t1,
								ValidTimeEnd:   &t3,
								Value:          "Old",
							},
						},
						// before update in transaction time
						{
							readOpts: []ReadOpt{AsOfTransactionTime(t2)},
							expectValue: &VersionedKV{
								Key:            "A",
								TxTimeStart:    t1,
								TxTimeEnd:      &t3,
								ValidTimeStart: t1,
								ValidTimeEnd:   nil,
								Value:          "Old",
							},
						},
					},
				},
				{
					desc:      "set w/ valid time end. original record overhands on both sides",
					now:       &t4,
					writeOpts: []WriteOpt{WithValidTime(t2), WithEndValidTime(t3)},
					key:       "A",
					findChecks: []findCheck{
						// query as of now for valid time and transaction time. change not visible
						{
							expectValue: &VersionedKV{
								Key:            "A",
								TxTimeStart:    t4,
								TxTimeEnd:      nil,
								ValidTimeStart: t3,
								ValidTimeEnd:   nil,
								Value:          "Old",
							},
						},
						// query as of now for transaction time, before update for valid time. change not visible
						{
							readOpts: []ReadOpt{AsOfValidTime(t1)},
							expectValue: &VersionedKV{
								Key:            "A",
								TxTimeStart:    t4,
								TxTimeEnd:      nil,
								ValidTimeStart: t1,
								ValidTimeEnd:   &t2,
								Value:          "Old",
							},
						},
						// query as of now for valid time, before update for transaction time. change not visible
						{
							readOpts: []ReadOpt{AsOfTransactionTime(t2)},
							expectValue: &VersionedKV{
								Key:            "A",
								TxTimeStart:    t1,
								TxTimeEnd:      &t4,
								ValidTimeStart: t1,
								ValidTimeEnd:   nil,
								Value:          "Old",
							},
						},
						// query as of valid time in range, transaction time after update. change visible
						{
							readOpts:          []ReadOpt{AsOfValidTime(t2), AsOfTransactionTime(t5)},
							expectErrNotFound: true,
						},
					},
				},
				{
					desc:      "set w/ valid time end. no overhang",
					now:       &t4,
					writeOpts: []WriteOpt{WithValidTime(t1)},
					key:       "A",
					findChecks: []findCheck{
						// query as of now for valid time and transaction time. change visible
						{
							expectErrNotFound: true,
						},
						// query as of now for valid time, before update for transaction time. change not visible
						{
							readOpts: []ReadOpt{AsOfTransactionTime(t2)},
							expectValue: &VersionedKV{
								Key:            "A",
								TxTimeStart:    t1,
								TxTimeEnd:      &t4,
								ValidTimeStart: t1,
								ValidTimeEnd:   nil,
								Value:          "Old",
							},
						},
					},
				},
			},
		},
		{
			fixtures: fixtures{
				name: "existing entries. multiple valid time ranges active",
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
			},
			testCases: []testCase{
				{
					desc:      "set overlaps multiple versions",
					now:       &t4,
					key:       "A",
					writeOpts: []WriteOpt{WithValidTime(t2), WithEndValidTime(t4)},
					findChecks: []findCheck{
						// TT = t5, VT = t4. after update transaction, not in valid range. too high
						{
							readOpts: []ReadOpt{AsOfTransactionTime(t5)},
							expectValue: &VersionedKV{
								Key:            "A",
								TxTimeStart:    t4,
								TxTimeEnd:      nil,
								ValidTimeStart: t4,
								ValidTimeEnd:   nil,
								Value:          "New",
							},
						},
						// TT = t5, VT = t1. after update transaction, not in valid range. too low
						{
							readOpts: []ReadOpt{AsOfTransactionTime(t5), AsOfValidTime(t1)},
							expectValue: &VersionedKV{
								Key:            "A",
								TxTimeStart:    t4,
								TxTimeEnd:      nil,
								ValidTimeStart: t1,
								ValidTimeEnd:   &t2,
								Value:          "Old",
							},
						},
						// TT = t5, VT = t3. after update transaction, in valid range
						{
							readOpts:          []ReadOpt{AsOfTransactionTime(t5), AsOfValidTime(t3)},
							expectErrNotFound: true,
						},
						// TT = t3, VT = t2 before update transaction, in the fixture original range
						{
							readOpts: []ReadOpt{AsOfTransactionTime(t3), AsOfValidTime(t2)},
							expectValue: &VersionedKV{
								Key:            "A",
								TxTimeStart:    t3,
								TxTimeEnd:      &t4,
								ValidTimeStart: t1,
								ValidTimeEnd:   &t3,
								Value:          "Old",
							},
						},
						// TT = t3, VT = t4. before update transaction, in the fixture updated range
						{
							readOpts: []ReadOpt{AsOfTransactionTime(t3), AsOfValidTime(t4)},
							expectValue: &VersionedKV{
								Key:            "A",
								TxTimeStart:    t3,
								TxTimeEnd:      &t4,
								ValidTimeStart: t3,
								ValidTimeEnd:   nil,
								Value:          "New",
							},
						},
						// TT = t2, VT = t2. before 1st fixture update transaction
						{
							readOpts: []ReadOpt{AsOfTransactionTime(t2), AsOfValidTime(t2)},
							expectValue: &VersionedKV{
								Key:            "A",
								TxTimeStart:    t1,
								TxTimeEnd:      &t3,
								ValidTimeStart: t1,
								ValidTimeEnd:   nil,
								Value:          "Old",
							},
						},
					},
				},
			},
		},
	}
	for _, s := range testCaseSets {
		s := s
		for _, tC := range s.testCases {
			tC := tC
			t.Run(fmt.Sprintf("%v: %v", s.fixtures.name, tC.desc), func(t *testing.T) {
				db, err := memory.NewDB(s.fixtures.vKVs()...)
				require.Nil(t, err)
				if tC.now != nil {
					db.SetNow(*tC.now)
				}
				err = db.Delete(tC.key, tC.writeOpts...)
				if tC.expectErr {
					require.NotNil(t, err)
					return
				}
				require.Nil(t, err)

				for _, findCheck := range tC.findChecks {
					ret, err := db.Get(tC.key, findCheck.readOpts...)
					if findCheck.expectErrNotFound {
						require.ErrorIs(t, err, ErrNotFound)
						return
					}
					require.Nil(t, err)
					assert.Equal(t, findCheck.expectValue, ret)
				}
			})
		}
	}
}

func TestHistory(t *testing.T) {
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
		expectErrNotFound bool
		expectErr         bool // this is exclusive of ErrNotFound. this is for unexepcted errors
		expectValues      []*VersionedKV
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
					desc: "basic - return 1 version",
					key:  "A",
					expectValues: []*VersionedKV{
						{
							Key:            "A",
							TxTimeStart:    t1,
							TxTimeEnd:      nil,
							ValidTimeStart: t1,
							ValidTimeEnd:   nil,
							Value:          "Old",
						},
					},
				},
			},
		},
		{
			fixtures: valuesSingleSetWithEnd,
			testCases: []testCase{
				{
					desc: "basic - return 1 version",
					key:  "A",
					expectValues: []*VersionedKV{
						{
							Key:            "A",
							TxTimeStart:    t1,
							TxTimeEnd:      nil,
							ValidTimeStart: t1,
							ValidTimeEnd:   &t3,
							Value:          "Old",
						},
					},
				},
			},
		},
		{
			fixtures: valuesUpdated,
			testCases: []testCase{
				{
					desc: "return versions by descending end transaction time, descending end valid time",
					key:  "A",
					expectValues: []*VersionedKV{
						{
							Key:            "A",
							TxTimeStart:    t3,
							TxTimeEnd:      nil,
							ValidTimeStart: t3,
							ValidTimeEnd:   nil,
							Value:          "New",
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
							TxTimeStart:    t1,
							TxTimeEnd:      &t3,
							ValidTimeStart: t1,
							ValidTimeEnd:   nil,
							Value:          "Old",
						},
					},
				},
			},
		},
		{
			fixtures: valuesDeleted,
			testCases: []testCase{
				{
					desc: "returns \"deleted\" versions",
					key:  "A",
					expectValues: []*VersionedKV{
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
							TxTimeStart:    t1,
							TxTimeEnd:      &t3,
							ValidTimeStart: t1,
							ValidTimeEnd:   nil,
							Value:          "Old",
						},
					},
				},
			},
		},
		{
			fixtures: fixtures{
				name: "version has later transaction time start, but earlier transaction time end",
				vKVs: func() []*VersionedKV {
					return []*VersionedKV{
						{
							Key:            "A",
							TxTimeStart:    t2,
							TxTimeEnd:      &t3,
							ValidTimeStart: t3,
							ValidTimeEnd:   &t4,
							Value:          "Old",
						},
						{
							Key:            "A",
							TxTimeStart:    t1,
							TxTimeEnd:      nil,
							ValidTimeStart: t1,
							ValidTimeEnd:   &t2,
							Value:          "New",
						},
					}
				},
			},
			testCases: []testCase{
				{
					desc: "return versions by descending end transaction time, descending end valid time",
					key:  "A",
					expectValues: []*VersionedKV{
						{
							Key:            "A",
							TxTimeStart:    t1,
							TxTimeEnd:      nil,
							ValidTimeStart: t1,
							ValidTimeEnd:   &t2,
							Value:          "New",
						},
						{
							Key:            "A",
							TxTimeStart:    t2,
							TxTimeEnd:      &t3,
							ValidTimeStart: t3,
							ValidTimeEnd:   &t4,
							Value:          "Old",
						},
					},
				},
			},
		},
		{
			fixtures: fixtures{
				name: "multiple versions have nil end transaction time",
				vKVs: func() []*VersionedKV {
					return []*VersionedKV{
						{
							Key:            "A",
							TxTimeStart:    t1,
							TxTimeEnd:      nil,
							ValidTimeStart: t1,
							ValidTimeEnd:   &t2,
							Value:          "New",
						},
						{
							Key:            "A",
							TxTimeStart:    t2,
							TxTimeEnd:      nil,
							ValidTimeStart: t3,
							ValidTimeEnd:   &t4,
							Value:          "Old",
						},
					}
				},
			},
			testCases: []testCase{
				{
					desc: "return versions by descending end transaction time, descending end valid time",
					key:  "A",
					expectValues: []*VersionedKV{
						{
							Key:            "A",
							TxTimeStart:    t2,
							TxTimeEnd:      nil,
							ValidTimeStart: t3,
							ValidTimeEnd:   &t4,
							Value:          "Old",
						},
						{
							Key:            "A",
							TxTimeStart:    t1,
							TxTimeEnd:      nil,
							ValidTimeStart: t1,
							ValidTimeEnd:   &t2,
							Value:          "New",
						},
					},
				},
			},
		},
	}
	for _, s := range testCaseSets {
		s := s
		for _, tC := range s.testCases {
			tC := tC
			t.Run(fmt.Sprintf("%v: %v", s.fixtures.name, tC.desc), func(t *testing.T) {
				db, err := memory.NewDB(s.fixtures.vKVs()...)
				require.Nil(t, err)
				ret, err := db.History(tC.key)
				if tC.expectErrNotFound {
					require.ErrorIs(t, err, ErrNotFound)
					return
				} else if tC.expectErr {
					require.NotErrorIs(t, err, ErrNotFound)
					require.NotNil(t, err)
					return
				}
				require.Nil(t, err)
				assert.Equal(t, tC.expectValues, ret)
			})
		}
	}
}

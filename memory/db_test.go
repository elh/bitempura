package memory_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	. "github.com/elh/bitempura"
	"github.com/elh/bitempura/dbtest"
	"github.com/elh/bitempura/memory"
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
				_, err := memory.NewDB(memory.WithVersionedKVs(s.fixtures.vKVs()))
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
	dbtest.TestGet(t, func(kvs []*VersionedKV) (DB, error) {
		return memory.NewDB(memory.WithVersionedKVs(kvs))
	})
}

func TestList(t *testing.T) {
	dbtest.TestList(t, func(kvs []*VersionedKV) (DB, error) {
		return memory.NewDB(memory.WithVersionedKVs(kvs))
	})
}

func TestSet(t *testing.T) {
	dbtest.TestSet(t, func(kvs []*VersionedKV, clock Clock) (DB, error) {
		return memory.NewDB(memory.WithVersionedKVs(kvs), memory.WithClock(clock))
	})
}

func TestDelete(t *testing.T) {
	dbtest.TestDelete(t, func(kvs []*VersionedKV, clock Clock) (DB, error) {
		return memory.NewDB(memory.WithVersionedKVs(kvs), memory.WithClock(clock))
	})
}

func TestHistory(t *testing.T) {
	dbtest.TestHistory(t, func(kvs []*VersionedKV) (DB, error) {
		return memory.NewDB(memory.WithVersionedKVs(kvs))
	})
}

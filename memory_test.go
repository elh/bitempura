package bitemporal_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	. "github.com/elh/bitemporal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	shortForm = "2006-Jan-02" // simple time format

	// these test dates are always in the real-world past
	t0 = mustParseTime(shortForm, "2021-Dec-31")
	t1 = mustParseTime(shortForm, "2022-Jan-01")
	t2 = mustParseTime(shortForm, "2022-Jan-02")
	t3 = mustParseTime(shortForm, "2022-Jan-03")
	t4 = mustParseTime(shortForm, "2022-Jan-04")
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

func TestFind(t *testing.T) {
	type fixtures struct {
		name      string
		documents map[string][]*Document
	}

	put1Attrs := Attributes{
		"score": 100,
	}
	put2Attrs := Attributes{
		"score": 200,
	}
	// 1 initial put
	aDocsSinglePut := fixtures{
		name: "single put, no end",
		documents: map[string][]*Document{
			"A": {
				{
					ID:             "A",
					TxTimeStart:    t1,
					TxTimeEnd:      nil,
					ValidTimeStart: t1,
					ValidTimeEnd:   nil,
					Attributes:     put1Attrs,
				},
			},
		},
	}
	// 1 initial put with a valid time end
	aDocsSinglePutWithEnd := fixtures{
		name: "single put, with end",
		documents: map[string][]*Document{
			"A": {
				{
					ID:             "A",
					TxTimeStart:    t1,
					TxTimeEnd:      nil,
					ValidTimeStart: t1,
					ValidTimeEnd:   &t3,
					Attributes:     put1Attrs,
				},
			},
		},
	}
	// // 1 initial put and 1 put with later valid time updating score
	// // this sets a TxTimeEnd for the initial record and creates 2 new ones
	aDocsUpdated := fixtures{
		name: "initial put, and then put with later valid time",
		documents: map[string][]*Document{
			"A": {
				{
					ID:             "A",
					TxTimeStart:    t1,
					TxTimeEnd:      &t3,
					ValidTimeStart: t1,
					ValidTimeEnd:   nil,
					Attributes:     put1Attrs,
				},
				{
					ID:             "A",
					TxTimeStart:    t3,
					TxTimeEnd:      nil,
					ValidTimeStart: t1,
					ValidTimeEnd:   &t3,
					Attributes:     put1Attrs,
				},
				{
					ID:             "A",
					TxTimeStart:    t3,
					TxTimeEnd:      nil,
					ValidTimeStart: t3,
					ValidTimeEnd:   nil,
					Attributes:     put2Attrs,
				},
			},
		},
	}
	aDocsDeleted := fixtures{
		name: "initial put, and then deletion with later valid time",
		documents: map[string][]*Document{
			"A": {
				{
					ID:             "A",
					TxTimeStart:    t1,
					TxTimeEnd:      &t3,
					ValidTimeStart: t1,
					ValidTimeEnd:   nil,
					Attributes:     put1Attrs,
				},
				{
					ID:             "A",
					TxTimeStart:    t3,
					TxTimeEnd:      nil,
					ValidTimeStart: t1,
					ValidTimeEnd:   &t3,
					Attributes:     put1Attrs,
				},
			},
		},
	}

	type testCase struct {
		desc              string
		id                string
		readOpts          []ReadOpt
		expectErrNotFound bool
		expectErr         bool // this is exclusive of ErrNotFound. this is for unexepcted errors
		expectAttributes  Attributes
	}

	testCaseSets := []struct {
		fixtures  fixtures
		testCases []testCase
	}{
		{
			fixtures: fixtures{
				name:      "empty db",
				documents: nil,
			},
			testCases: []testCase{
				{
					desc:              "not found",
					id:                "A",
					expectErrNotFound: true,
				},
			},
		},
		{
			fixtures: aDocsSinglePut,
			testCases: []testCase{
				{
					desc:             "found - default as of times",
					id:               "A",
					expectAttributes: put1Attrs,
				},
				{
					desc:              "not found - as of valid time T before valid time start",
					id:                "A",
					readOpts:          []ReadOpt{AsOfValidTime(t0)},
					expectErrNotFound: true,
				},
				{
					desc:              "not found - as of tx time T before tx time start",
					id:                "A",
					readOpts:          []ReadOpt{AsOfTransactionTime(t0)},
					expectErrNotFound: true,
				},
				{
					desc:             "found - as of valid time T in range",
					id:               "A",
					readOpts:         []ReadOpt{AsOfValidTime(t2)},
					expectAttributes: put1Attrs,
				},
				{
					desc:             "found - as of tx time T in range",
					id:               "A",
					readOpts:         []ReadOpt{AsOfTransactionTime(t2)},
					expectAttributes: put1Attrs,
				},
				{
					desc:             "found - as of valid time T in range (inclusive)",
					id:               "A",
					readOpts:         []ReadOpt{AsOfValidTime(t1)},
					expectAttributes: put1Attrs,
				},
				{
					desc:             "found - as of tx time T in range (inclusive)",
					id:               "A",
					readOpts:         []ReadOpt{AsOfTransactionTime(t1)},
					expectAttributes: put1Attrs,
				},
			},
		},
		{
			fixtures: aDocsSinglePutWithEnd,
			testCases: []testCase{
				{
					desc:             "found - as of valid and tx time T in range",
					id:               "A",
					readOpts:         []ReadOpt{AsOfValidTime(t2), AsOfTransactionTime(t2)},
					expectAttributes: put1Attrs,
				},
				// valid time end range
				{
					desc:              "not found - default as of times",
					id:                "A",
					expectErrNotFound: true,
				},
				{
					desc:              "not found - as of valid time T after valid time end",
					id:                "A",
					readOpts:          []ReadOpt{AsOfValidTime(t4)},
					expectErrNotFound: true,
				},
				{
					desc:              "not found - as of valid time T equal to valid time end (exclusive)",
					id:                "A",
					readOpts:          []ReadOpt{AsOfValidTime(t3)},
					expectErrNotFound: true,
				},
			},
		},
		{
			fixtures: aDocsUpdated,
			testCases: []testCase{
				{
					desc:             "found - default as of times",
					id:               "A",
					expectAttributes: put2Attrs,
				},
				{
					desc:             "as of tx time now, as of valid time before update",
					id:               "A",
					readOpts:         []ReadOpt{AsOfValidTime(t1)},
					expectAttributes: put1Attrs,
				},
				{
					desc:             "as of tx time before update, as of valid time now",
					id:               "A",
					readOpts:         []ReadOpt{AsOfTransactionTime(t1)},
					expectAttributes: put1Attrs,
				},
				{
					desc:             "as of tx time before update, as of valid time before update",
					id:               "A",
					readOpts:         []ReadOpt{AsOfValidTime(t1), AsOfTransactionTime(t1)},
					expectAttributes: put1Attrs,
				},
			},
		},
		{
			fixtures: aDocsDeleted,
			testCases: []testCase{
				{
					desc:              "not found - default as of times",
					id:                "A",
					expectErrNotFound: true,
				},
				{
					desc:             "as of tx time now, as of valid time before update",
					id:               "A",
					readOpts:         []ReadOpt{AsOfValidTime(t1)},
					expectAttributes: put1Attrs,
				},
				{
					desc:             "as of tx time before update, as of valid time now",
					id:               "A",
					readOpts:         []ReadOpt{AsOfTransactionTime(t1)},
					expectAttributes: put1Attrs,
				},
				{
					desc:             "as of tx time before update, as of valid time before update",
					id:               "A",
					readOpts:         []ReadOpt{AsOfValidTime(t1), AsOfTransactionTime(t1)},
					expectAttributes: put1Attrs,
				},
			},
		},
	}
	for _, s := range testCaseSets {
		s := s
		for _, tC := range s.testCases {
			tC := tC
			t.Run(fmt.Sprintf("%v: %v", s.fixtures.name, tC.desc), func(t *testing.T) {
				db := NewMemoryDB(s.fixtures.documents)
				ret, err := db.Find(tC.id, tC.readOpts...)
				if tC.expectErrNotFound {
					require.ErrorIs(t, err, ErrNotFound)
					return
				} else if tC.expectErr {
					require.NotErrorIs(t, err, ErrNotFound)
					require.NotNil(t, err)
					return
				}
				require.Nil(t, err)
				assert.Equal(t, tC.expectAttributes, ret.Attributes)
			})
		}
	}
}

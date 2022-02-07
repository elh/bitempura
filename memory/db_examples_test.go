package memory_test

import (
	"sort"
	"testing"

	. "github.com/elh/bitempura"
	"github.com/elh/bitempura/dbtest"
	"github.com/elh/bitempura/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Case Study Tests
// XTDB: see https://docs.xtdb.com/concepts/bitemporality/
// Robinhood: see https://robinhood.engineering/tracking-temporal-data-at-robinhood-b62291644a31

// XTDB Bitemporality > Example Queries > Crime Investigations
// see https://docs.xtdb.com/concepts/bitemporality/
// > The paper then lists a sequence of entry and departure events at various United States border checkpoints. We as
// > the investigator will step through this sequence to monitor a set of suspects. These events will arrive in an
// > undetermined chronological order based on how and when each checkpoint is able to manually relay the information.
func TestTXDBCrimeInvestigationExample(t *testing.T) {
	clock := &dbtest.TestClock{}
	db, err := memory.NewDB(memory.WithClock(clock))
	require.Nil(t, err)
	keys := []string{"p1", "p2", "p3", "p4", "p5", "p6", "p7"}
	defer dbtest.WriteOutputHistory(db, keys, t.Name())

	type Doc map[string]interface{}

	// -------------------- Day 0 --------------------
	// The first document shows that Person 2 was recorded entering via :SFO and the second document shows that Person 3
	// was recorded entering :LA.
	day0 := mustParseTime(shortForm, "2018-12-31")
	require.Nil(t, clock.SetNow(day0))
	require.Nil(t, db.Set("p2", Doc{
		"entry-pt":       "SFO",
		"arrival-time":   day0,
		"departure-time": nil,
	}))
	require.Nil(t, db.Set("p3", Doc{
		"entry-pt":       "LA",
		"arrival-time":   day0,
		"departure-time": nil,
	}))

	// -------------------- Day 1 --------------------
	// No new recorded events arrive on Day 1 (#inst "2019-01-01"), so there are no documents available to ingest.

	// -------------------- Day 2 --------------------
	// A single event arrives on Day 2 showing Person 4 arriving at :NY:
	day2 := day0.AddDate(0, 0, 2)
	require.Nil(t, clock.SetNow(day2))
	require.Nil(t, db.Set("p4", Doc{
		"entry-pt":       "NY",
		"arrival-time":   day2,
		"departure-time": nil,
	}))

	// -------------------- Day 3 --------------------
	// Next, we learn on Day 3 that Person 4 departed from :NY, which is represented as an update to the existing
	// document using the Day 3 valid time:
	day3 := day0.AddDate(0, 0, 3)
	require.Nil(t, clock.SetNow(day3))
	require.Nil(t, db.Set("p4", Doc{
		"entry-pt":       "NY",
		"arrival-time":   day2,
		"departure-time": day3,
	}))

	// -------------------- Day 4 --------------------
	// On Day 4 we begin to receive events relating to the previous days of the investigation. First we receive an event
	// showing that Person 1 entered :NY on Day 0 which must ingest using the Day 0 valid time #inst "2018-12-31":
	day4 := day0.AddDate(0, 0, 4)
	require.Nil(t, clock.SetNow(day4))
	require.Nil(t, db.Set("p1", Doc{
		"entry-pt":       "NY",
		"arrival-time":   day0,
		"departure-time": nil,
	},
		WithValidTime(day0)))
	// We then receive an event showing that Person 1 departed from :NY on Day 3, so again we ingest this document using
	// the corresponding Day 3 valid time:
	require.Nil(t, db.Set("p1", Doc{
		"entry-pt":       "NY",
		"arrival-time":   day0,
		"departure-time": day3,
	},
		WithValidTime(day3)))
	// Finally, we receive two events relating to Day 4, which can be ingested using the current valid time:
	require.Nil(t, db.Set("p1", Doc{
		"entry-pt":       "LA",
		"arrival-time":   day4,
		"departure-time": nil,
	}))
	require.Nil(t, db.Set("p3", Doc{
		"entry-pt":       "LA",
		"arrival-time":   day0,
		"departure-time": day4,
	}))

	// -------------------- Day 5 --------------------
	// On Day 5 there is an event showing that Person 2, having arrived on Day 0 (which we already knew), departed from
	// :SFO on Day 5.
	day5 := day0.AddDate(0, 0, 5)
	require.Nil(t, clock.SetNow(day5))
	require.Nil(t, db.Set("p2", Doc{
		"entry-pt":       "SFO",
		"arrival-time":   day0,
		"departure-time": day5,
	}))

	// -------------------- Day 6 --------------------
	// No new recorded events arrive on Day 6 (#inst "2019-01-06"), so there are no documents available to ingest.

	// -------------------- Day 7 --------------------
	// On Day 7 two documents arrive. The first document corrects the previous assertion that Person 3 departed on
	// Day 4, which was misrecorded due to human error. The second document shows that Person 3 has only just departed
	// on Day 7, which is how the previous error was noticed.
	day7 := day0.AddDate(0, 0, 7)
	require.Nil(t, clock.SetNow(day7))
	require.Nil(t, db.Set("p3", Doc{
		"entry-pt":       "LA",
		"arrival-time":   day0,
		"departure-time": nil,
	},
		WithValidTime(day4)))
	require.Nil(t, db.Set("p3", Doc{
		"entry-pt":       "LA",
		"arrival-time":   day0,
		"departure-time": day7,
	}))

	// -------------------- Day 8 --------------------
	// Two documents have been received relating to new arrivals on Day 8. Note that Person 3 has arrived back in the
	// country again.
	day8 := day0.AddDate(0, 0, 8)
	require.Nil(t, clock.SetNow(day8))
	require.Nil(t, db.Set("p3", Doc{
		"entry-pt":       "SFO",
		"arrival-time":   day8,
		"departure-time": nil,
	}))
	require.Nil(t, db.Set("p4", Doc{
		"entry-pt":       "LA",
		"arrival-time":   day8,
		"departure-time": nil,
	}))

	// -------------------- Day 9 --------------------
	// On Day 9 we learn that Person 3 also departed on Day 8.
	day9 := day0.AddDate(0, 0, 9)
	require.Nil(t, clock.SetNow(day9))
	require.Nil(t, db.Set("p3", Doc{
		"entry-pt":       "SFO",
		"arrival-time":   day8,
		"departure-time": day8,
	}))

	// -------------------- Day 10 --------------------
	// A single document arrives showing that Person 5 entered at :LA earlier that day.
	day10 := day0.AddDate(0, 0, 10)
	require.Nil(t, clock.SetNow(day10))
	require.Nil(t, db.Set("p5", Doc{
		"entry-pt":       "LA",
		"arrival-time":   day10,
		"departure-time": nil,
	}))

	// -------------------- Day 11 --------------------
	// Similarly to the previous day, a single document arrives showing that Person 7 entered at :NY earlier that day.
	day11 := day0.AddDate(0, 0, 11)
	require.Nil(t, clock.SetNow(day11))
	require.Nil(t, db.Set("p7", Doc{
		"entry-pt":       "NY",
		"arrival-time":   day11,
		"departure-time": nil,
	}))

	// -------------------- Day 12 --------------------
	// Finally, on Day 12 we learn that Person 6 entered at :NY that same day.
	day12 := day0.AddDate(0, 0, 12)
	require.Nil(t, clock.SetNow(day11))
	require.Nil(t, db.Set("p6", Doc{
		"entry-pt":       "NY",
		"arrival-time":   day12,
		"departure-time": nil,
	}))

	// -------------------- Question Time --------------------
	// Find all persons who are known to be present in the United States on day 2 (valid time), as of
	// day 3 (transaction time).
	//
	// The answer given by XTDB is a simple set of the three relevant people along with the details of their last entry
	// and confirmation that none of them were known to have yet departed at this point:
	// #{[:p2 :SFO #inst "2018-12-31" :na]
	//   [:p3 :LA #inst "2018-12-31" :na]
	//   [:p4 :NY #inst "2019-01-02" :na]}
	out, err := db.List(AsOfValidTime(day2), AsOfTransactionTime(day3))
	require.Nil(t, err)
	require.Len(t, out, 3)
	outByKey := sortKVsByKey(out)
	assert.Equal(t, "p2", outByKey[0].Key)
	assert.Equal(t, Doc{
		"entry-pt":       "SFO",
		"arrival-time":   day0,
		"departure-time": nil,
	}, outByKey[0].Value)
	assert.Equal(t, "p3", outByKey[1].Key)
	assert.Equal(t, Doc{
		"entry-pt":       "LA",
		"arrival-time":   day0,
		"departure-time": nil,
	}, outByKey[1].Value)
	assert.Equal(t, "p4", outByKey[2].Key)
	assert.Equal(t, Doc{
		"entry-pt":       "NY",
		"arrival-time":   day2,
		"departure-time": nil,
	}, outByKey[2].Value)

	// -------------------- My extra tests --------------------
	// elh: this was actually quite simple. the times are so early that they disregard so many edits. let's do a few
	// related checks.

	// ^ same valid time as example but as of transaction time now (VT = day 2, TT = day 12)
	out, err = db.List(AsOfValidTime(day2))
	require.Nil(t, err)
	require.Len(t, out, 4)
	outByKey = sortKVsByKey(out)
	assert.Equal(t, "p1", outByKey[0].Key) // this was not known in the original query. p1 info was recorded TT = day 4
	assert.Equal(t, Doc{
		"entry-pt":       "NY",
		"arrival-time":   day0,
		"departure-time": nil,
	}, outByKey[0].Value)
	assert.Equal(t, "p2", outByKey[1].Key)
	assert.Equal(t, Doc{
		"entry-pt":       "SFO",
		"arrival-time":   day0,
		"departure-time": nil,
	}, outByKey[1].Value)
	assert.Equal(t, "p3", outByKey[2].Key)
	assert.Equal(t, Doc{
		"entry-pt":       "LA",
		"arrival-time":   day0,
		"departure-time": nil,
	}, outByKey[2].Value)
	assert.Equal(t, "p4", outByKey[3].Key)
	assert.Equal(t, Doc{
		"entry-pt":       "NY",
		"arrival-time":   day2,
		"departure-time": nil,
	}, outByKey[3].Value)

	// state of db at now (VT = day 12, TT = day 12)
	out, err = db.List()
	require.Nil(t, err)
	require.Len(t, out, 7)
	outByKey = sortKVsByKey(out)
	assert.Equal(t, "p1", outByKey[0].Key)
	assert.Equal(t, Doc{
		"entry-pt":       "LA",
		"arrival-time":   day4,
		"departure-time": nil,
	}, outByKey[0].Value)
	assert.Equal(t, "p2", outByKey[1].Key)
	assert.Equal(t, Doc{
		"entry-pt":       "SFO",
		"arrival-time":   day0,
		"departure-time": day5,
	}, outByKey[1].Value)
	assert.Equal(t, "p3", outByKey[2].Key)
	assert.Equal(t, Doc{
		"entry-pt":       "SFO",
		"arrival-time":   day8,
		"departure-time": day8,
	}, outByKey[2].Value)
	assert.Equal(t, "p4", outByKey[3].Key)
	assert.Equal(t, Doc{
		"entry-pt":       "LA",
		"arrival-time":   day8,
		"departure-time": nil,
	}, outByKey[3].Value)
	assert.Equal(t, "p5", outByKey[4].Key)
	assert.Equal(t, Doc{
		"entry-pt":       "LA",
		"arrival-time":   day10,
		"departure-time": nil,
	}, outByKey[4].Value)
	assert.Equal(t, "p6", outByKey[5].Key)
	assert.Equal(t, Doc{
		"entry-pt":       "NY",
		"arrival-time":   day12,
		"departure-time": nil,
	}, outByKey[5].Value)
	assert.Equal(t, "p7", outByKey[6].Key)
	assert.Equal(t, Doc{
		"entry-pt":       "NY",
		"arrival-time":   day11,
		"departure-time": nil,
	}, outByKey[6].Value)
}

// Robinhood Eng blog > Tracking Temporal Data at Robinhood
// see https://robinhood.engineering/tracking-temporal-data-at-robinhood-b62291644a31
// > At Robinhood, accounting is a central part of our business...
func TestRobinhoodExample(t *testing.T) {
	clock := &dbtest.TestClock{}
	db, err := memory.NewDB(memory.WithClock(clock))
	require.Nil(t, err)
	defer dbtest.WriteOutputHistory(db, []string{"user-1"}, t.Name())

	type Balance map[string]interface{}

	// Say you deposit $100 in your account on 3/14.
	mar14 := mustParseTime(shortForm, "2021-03-14")
	require.Nil(t, clock.SetNow(mar14))
	require.Nil(t, db.Set("user-1", Balance{
		"cash-balance": 100,
		"description":  "Deposit", // description of last event??
	}))
	// On 3/20, you purchase 1 share of ABC stock at $25.
	mar20 := mustParseTime(shortForm, "2021-03-20")
	require.Nil(t, clock.SetNow(mar20))
	require.Nil(t, db.Set("user-1", Balance{
		"cash-balance": 75,
		"description":  "Stock Purchase",
	}))
	// On 3/21, Robinhood received a price improvement, indicating the execution for your 1 share of ABC was
	// actually $10.
	mar21 := mustParseTime(shortForm, "2021-03-21")
	require.Nil(t, clock.SetNow(mar21))
	require.Nil(t, db.Set("user-1", Balance{
		"cash-balance": 90,
		"description":  "Price Improvement",
	},
		WithValidTime(mar20)))

	// compacting...
	findBalance := func(opts ...ReadOpt) interface{} {
		ret, err := db.Get("user-1", opts...)
		require.Nil(t, err)
		return ret.Value.(Balance)["cash-balance"]
	}
	expectErrGetBalance := func(opts ...ReadOpt) {
		_, err := db.Get("user-1", opts...)
		require.NotNil(t, err)
	}

	// elh: now let's check the price at interesting points. see their diagram
	mar13 := mustParseTime(shortForm, "2021-03-13") // before any VT, TT
	// VT=now, TT=now. as of now
	assert.Equal(t, 90, findBalance())
	// VT=now, TT=3/20. before price correction
	assert.Equal(t, 75, findBalance(AsOfTransactionTime(mar20)))
	// VT=now, TT=3/14. before stock purchase
	assert.Equal(t, 100, findBalance(AsOfTransactionTime(mar14)))
	// VT=now, TT=3/13. before any record
	expectErrGetBalance(AsOfTransactionTime(mar13))
	// VT=3/14, TT=now. 3/14 balance as of now
	assert.Equal(t, 100, findBalance(AsOfValidTime(mar14)))
	// VT=3/14, TT=3/20. 3/14 balance before price correction
	assert.Equal(t, 100, findBalance(AsOfTransactionTime(mar20), AsOfValidTime(mar14)))
	// VT=3/14, TT=3/14. 3/14 balance before stock purchase
	assert.Equal(t, 100, findBalance(AsOfTransactionTime(mar14), AsOfValidTime(mar14)))
	// VT=3/14, TT=3/13. 3/14 balance before any record
	expectErrGetBalance(AsOfTransactionTime(mar13), AsOfValidTime(mar14))
}

func sortKVsByKey(ds []*VersionedKV) []*VersionedKV {
	out := make([]*VersionedKV, len(ds))
	copy(out, ds)
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

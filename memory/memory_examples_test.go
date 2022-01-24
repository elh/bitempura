package memory_test

import (
	"testing"

	. "github.com/elh/bitemporal"
	"github.com/elh/bitemporal/memory"
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
	db := memory.NewDB(nil)

	// -------------------- Day 0 --------------------
	// The first document shows that Person 2 was recorded entering via :SFO and the second document shows that Person 3
	// was recorded entering :LA.
	day0 := mustParseTime(shortForm, "2018-12-31")
	db.SetNow(day0)
	require.Nil(t, db.Put("p2", Attributes{
		"entry-pt":       "SFO",
		"arrival-time":   day0,
		"departure-time": nil,
	}))
	require.Nil(t, db.Put("p3", Attributes{
		"entry-pt":       "LA",
		"arrival-time":   day0,
		"departure-time": nil,
	}))

	// -------------------- Day 1 --------------------
	// No new recorded events arrive on Day 1 (#inst "2019-01-01"), so there are no documents available to ingest.

	// -------------------- Day 2 --------------------
	// A single event arrives on Day 2 showing Person 4 arriving at :NY:
	day2 := day0.AddDate(0, 0, 2)
	db.SetNow(day2)
	require.Nil(t, db.Put("p4", Attributes{
		"entry-pt":       "NY",
		"arrival-time":   day2,
		"departure-time": nil,
	}))

	// -------------------- Day 3 --------------------
	// Next, we learn on Day 3 that Person 4 departed from :NY, which is represented as an update to the existing
	// document using the Day 3 valid time:
	day3 := day0.AddDate(0, 0, 3)
	db.SetNow(day3)
	require.Nil(t, db.Put("p4", Attributes{
		"entry-pt":       "NY",
		"arrival-time":   day2,
		"departure-time": day3,
	}))

	// -------------------- Day 4 --------------------
	// On Day 4 we begin to receive events relating to the previous days of the investigation. First we receive an event
	// showing that Person 1 entered :NY on Day 0 which must ingest using the Day 0 valid time #inst "2018-12-31":
	day4 := day0.AddDate(0, 0, 4)
	db.SetNow(day4)
	require.Nil(t, db.Put("p1", Attributes{
		"entry-pt":       "NY",
		"arrival-time":   day0,
		"departure-time": nil,
	},
		WithValidTime(day0)))
	// We then receive an event showing that Person 1 departed from :NY on Day 3, so again we ingest this document using
	// the corresponding Day 3 valid time:
	require.Nil(t, db.Put("p1", Attributes{
		"entry-pt":       "NY",
		"arrival-time":   day0,
		"departure-time": day3,
	},
		WithValidTime(day3)))
	// Finally, we receive two events relating to Day 4, which can be ingested using the current valid time:
	require.Nil(t, db.Put("p1", Attributes{
		"entry-pt":       "LA",
		"arrival-time":   day4,
		"departure-time": nil,
	}))
	require.Nil(t, db.Put("p3", Attributes{
		"entry-pt":       "LA",
		"arrival-time":   day0,
		"departure-time": day4,
	}))

	// -------------------- Day 5 --------------------
	// On Day 5 there is an event showing that Person 2, having arrived on Day 0 (which we already knew), departed from
	// :SFO on Day 5.
	day5 := day0.AddDate(0, 0, 5)
	db.SetNow(day5)
	require.Nil(t, db.Put("p2", Attributes{
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
	db.SetNow(day7)
	require.Nil(t, db.Put("p3", Attributes{
		"entry-pt":       "LA",
		"arrival-time":   day0,
		"departure-time": nil,
	},
		WithValidTime(day4)))
	require.Nil(t, db.Put("p3", Attributes{
		"entry-pt":       "LA",
		"arrival-time":   day0,
		"departure-time": day7,
	}))

	// -------------------- Day 8 --------------------
	// Two documents have been received relating to new arrivals on Day 8. Note that Person 3 has arrived back in the
	// country again.
	day8 := day0.AddDate(0, 0, 8)
	db.SetNow(day8)
	require.Nil(t, db.Put("p3", Attributes{
		"entry-pt":       "SFO",
		"arrival-time":   day8,
		"departure-time": nil,
	}))
	require.Nil(t, db.Put("p4", Attributes{
		"entry-pt":       "LA",
		"arrival-time":   day8,
		"departure-time": nil,
	}))

	// -------------------- Day 9 --------------------
	// On Day 9 we learn that Person 3 also departed on Day 8.
	day9 := day0.AddDate(0, 0, 9)
	db.SetNow(day9)
	require.Nil(t, db.Put("p3", Attributes{
		"entry-pt":       "SFO",
		"arrival-time":   day8,
		"departure-time": day8,
	}))

	// -------------------- Day 10 --------------------
	// A single document arrives showing that Person 5 entered at :LA earlier that day.
	day10 := day0.AddDate(0, 0, 10)
	db.SetNow(day10)
	require.Nil(t, db.Put("p5", Attributes{
		"entry-pt":       "LA",
		"arrival-time":   day10,
		"departure-time": nil,
	}))

	// -------------------- Day 11 --------------------
	// Similarly to the previous day, a single document arrives showing that Person 7 entered at :NY earlier that day.
	day11 := day0.AddDate(0, 0, 11)
	db.SetNow(day11)
	require.Nil(t, db.Put("p7", Attributes{
		"entry-pt":       "NY",
		"arrival-time":   day11,
		"departure-time": nil,
	}))

	// -------------------- Day 12 --------------------
	// Finally, on Day 12 we learn that Person 6 entered at :NY that same day.
	day12 := day0.AddDate(0, 0, 12)
	db.SetNow(day11)
	require.Nil(t, db.Put("p6", Attributes{
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
	outByID := sortDocumentsByID(out)
	assert.Equal(t, "p2", outByID[0].ID)
	assert.Equal(t, Attributes{
		"entry-pt":       "SFO",
		"arrival-time":   day0,
		"departure-time": nil,
	}, outByID[0].Attributes)
	assert.Equal(t, "p3", outByID[1].ID)
	assert.Equal(t, Attributes{
		"entry-pt":       "LA",
		"arrival-time":   day0,
		"departure-time": nil,
	}, outByID[1].Attributes)
	assert.Equal(t, "p4", outByID[2].ID)
	assert.Equal(t, Attributes{
		"entry-pt":       "NY",
		"arrival-time":   day2,
		"departure-time": nil,
	}, outByID[2].Attributes)

	// -------------------- My extra tests --------------------
	// elh: this was actually quite simple. the times are so early that they disregard so many edits. let's do a few
	// related checks.

	// ^ same valid time as example but as of transaction time now (VT = day 2, TT = day 12)
	out, err = db.List(AsOfValidTime(day2))
	require.Nil(t, err)
	require.Len(t, out, 4)
	outByID = sortDocumentsByID(out)
	assert.Equal(t, "p1", outByID[0].ID) // this was not known in the original query. p1 info was recorded TT = day 4
	assert.Equal(t, Attributes{
		"entry-pt":       "NY",
		"arrival-time":   day0,
		"departure-time": nil,
	}, outByID[0].Attributes)
	assert.Equal(t, "p2", outByID[1].ID)
	assert.Equal(t, Attributes{
		"entry-pt":       "SFO",
		"arrival-time":   day0,
		"departure-time": nil,
	}, outByID[1].Attributes)
	assert.Equal(t, "p3", outByID[2].ID)
	assert.Equal(t, Attributes{
		"entry-pt":       "LA",
		"arrival-time":   day0,
		"departure-time": nil,
	}, outByID[2].Attributes)
	assert.Equal(t, "p4", outByID[3].ID)
	assert.Equal(t, Attributes{
		"entry-pt":       "NY",
		"arrival-time":   day2,
		"departure-time": nil,
	}, outByID[3].Attributes)

	// state of db at now (VT = day 12, TT = day 12)
	out, err = db.List()
	require.Nil(t, err)
	require.Len(t, out, 7)
	outByID = sortDocumentsByID(out)
	assert.Equal(t, "p1", outByID[0].ID)
	assert.Equal(t, Attributes{
		"entry-pt":       "LA",
		"arrival-time":   day4,
		"departure-time": nil,
	}, outByID[0].Attributes)
	assert.Equal(t, "p2", outByID[1].ID)
	assert.Equal(t, Attributes{
		"entry-pt":       "SFO",
		"arrival-time":   day0,
		"departure-time": day5,
	}, outByID[1].Attributes)
	assert.Equal(t, "p3", outByID[2].ID)
	assert.Equal(t, Attributes{
		"entry-pt":       "SFO",
		"arrival-time":   day8,
		"departure-time": day8,
	}, outByID[2].Attributes)
	assert.Equal(t, "p4", outByID[3].ID)
	assert.Equal(t, Attributes{
		"entry-pt":       "LA",
		"arrival-time":   day8,
		"departure-time": nil,
	}, outByID[3].Attributes)
	assert.Equal(t, "p5", outByID[4].ID)
	assert.Equal(t, Attributes{
		"entry-pt":       "LA",
		"arrival-time":   day10,
		"departure-time": nil,
	}, outByID[4].Attributes)
	assert.Equal(t, "p6", outByID[5].ID)
	assert.Equal(t, Attributes{
		"entry-pt":       "NY",
		"arrival-time":   day12,
		"departure-time": nil,
	}, outByID[5].Attributes)
	assert.Equal(t, "p7", outByID[6].ID)
	assert.Equal(t, Attributes{
		"entry-pt":       "NY",
		"arrival-time":   day11,
		"departure-time": nil,
	}, outByID[6].Attributes)
}

// Robinhood Eng blog > Tracking Temporal Data at Robinhood
// see https://robinhood.engineering/tracking-temporal-data-at-robinhood-b62291644a31
// > At Robinhood, accounting is a central part of our business...
func TestRobinhoodExample(t *testing.T) {
	db := memory.NewDB(nil)

	// Say you deposit $100 in your account on 3/14.
	mar14 := mustParseTime(shortForm, "2021-03-14")
	db.SetNow(mar14)
	require.Nil(t, db.Put("user-1", Attributes{
		"cash-balance": 100,
		"description":  "Deposit", // description of last event??
	}))
	// On 3/20, you purchase 1 share of ABC stock at $25.
	mar20 := mustParseTime(shortForm, "2021-03-20")
	db.SetNow(mar20)
	require.Nil(t, db.Put("user-1", Attributes{
		"cash-balance": 75,
		"description":  "Stock Purchase",
	}))
	// On 3/21, Robinhood received a price improvement, indicating the execution for your 1 share of ABC was
	// actually $10.
	mar21 := mustParseTime(shortForm, "2021-03-21")
	db.SetNow(mar21)
	require.Nil(t, db.Put("user-1", Attributes{
		"cash-balance": 90,
		"description":  "Price Improvement",
	},
		WithValidTime(mar20)))

	// compacting...
	findBalance := func(opts ...ReadOpt) interface{} {
		ret, err := db.Find("user-1", opts...)
		require.Nil(t, err)
		return ret.Attributes["cash-balance"]
	}
	expectErrFindBalance := func(opts ...ReadOpt) {
		_, err := db.Find("user-1", opts...)
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
	expectErrFindBalance(AsOfTransactionTime(mar13))
	// VT=3/14, TT=now. 3/14 balance as of now
	assert.Equal(t, 100, findBalance(AsOfValidTime(mar14)))
	// VT=3/14, TT=3/20. 3/14 balance before price correction
	assert.Equal(t, 100, findBalance(AsOfTransactionTime(mar20), AsOfValidTime(mar14)))
	// VT=3/14, TT=3/14. 3/14 balance before stock purchase
	assert.Equal(t, 100, findBalance(AsOfTransactionTime(mar14), AsOfValidTime(mar14)))
	// VT=3/14, TT=3/13. 3/14 balance before any record
	expectErrFindBalance(AsOfTransactionTime(mar13), AsOfValidTime(mar14))
}

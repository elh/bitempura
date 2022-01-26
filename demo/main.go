package main

import (
	"encoding/json"
	"fmt"
	"time"

	bt "github.com/elh/bitempura"
	"github.com/elh/bitempura/memory"
)

// TODO: would be cool if the demo on the first page was runnable in playground

// TODO: playground pins time to 2009 lol
// > In the playground the time begins at 2009-11-10 23:00:00 UTC (determining the significance of this date is an
// exercise for the reader). This makes it easier to cache programs by giving them deterministic output.

var (
	shortForm = "2006-01-02" // simple time format

	oneYAgo = mustParseTime(shortForm, "2019-01-01")
	dec30   = mustParseTime(shortForm, "2021-12-30")
	jan1    = mustParseTime(shortForm, "2022-01-01")
	jan3    = mustParseTime(shortForm, "2022-01-03")
	jan8    = mustParseTime(shortForm, "2022-01-08")
)

func main() {
	// We initialize a DB and start using it like an ordinary key-value store.
	db, err := memory.NewDB()
	panicIfErr(err)

	err = db.Set("Alice/balance", 1)
	panicIfErr(err)

	err = db.Set("Bob/balance", 100, bt.WithValidTime(oneYAgo))
	panicIfErr(err)

	val, err := db.Get("Bob/balance")
	panicIfErr(err)
	fmt.Println(toJSON(val))

	err = db.Delete("Alice/balance")
	panicIfErr(err)
	// and so on...

	// We later learn that Bob had a temporary pending charge we missed from Dec 30 to Jan 3. (VT start = Dec 30, VT end = Jan 3)
	// Retroactively record it! This does not change his balance today nor does it destroy any history we had about that period.
	err = db.Set("Bob/balance", 90, bt.WithValidTime(dec30), bt.WithEndValidTime(jan3))
	panicIfErr(err)

	// We can at any point seamlessly ask questions about the real world past AND database record past!
	// "What was Bob's balance on Jan 1 as best we knew on Jan 8?" (VT = Jan 1, TT = Jan 8)
	// valTimeTravel, err := db.Get("Bob/balance", bt.AsOfValidTime(jan1), bt.AsOfTransactionTime(jan8))
	// panicIfErr(err)
	// fmt.Println(toJSON(valTimeTravel))

	// More time passes and more corrections are made... When trying to make sense of what happened last month, we can ask again:
	// "But what was it on Jan 1 as best we now know?" (VT = Jan 1, TT = now)
	valTimeTravel2, err := db.Get("Bob/balance", bt.AsOfValidTime(jan1))
	panicIfErr(err)
	fmt.Println(toJSON(valTimeTravel2))

	// And while we are at it, let's double check all of our transactions and known states for Bob's balance.
	versions, err := db.History("Bob/balance")
	panicIfErr(err)
	fmt.Println(toJSON(versions))
}

func mustParseTime(layout, value string) time.Time {
	t, err := time.Parse(layout, value)
	if err != nil {
		panic(err)
	}
	return t
}

// TODO: better support for options...

// verboseDB prints to stdout all calls
// type verboseDB struct {
// 	wrapped bt.DB
// }

// // Get data by key (as of optional valid and transaction times).
// func (db *verboseDB) Get(key string, opts ...bt.ReadOpt) (*bt.VersionedKV, error) {
// 	str := fmt.Sprintf("Get %v", key) // TODO: ugh. need to clean up option parsing
// 	fmt.Println(str)
// 	return db.wrapped.Get(key, opts...)
// }

// // List all data (as of optional valid and transaction times).
// func (db *verboseDB) List(opts ...bt.ReadOpt) ([]*bt.VersionedKV, error) {
// 	return db.wrapped.List(opts...)
// }

// // Set stores value (with optional start and end valid time).
// func (db *verboseDB) Set(key string, value bt.Value, opts ...bt.WriteOpt) error {
// 	return db.wrapped.Set(key, value, opts...)
// }

// // Delete removes value (with optional start and end valid time).
// func (db *verboseDB) Delete(key string, opts ...bt.WriteOpt) error {
// 	return db.wrapped.Delete(key, opts...)
// }

// // History returns versions by descending end transaction time, descending end valid time
// func (db *verboseDB) History(key string) ([]*bt.VersionedKV, error) {
// 	return db.wrapped.History(key)
// }

func panicIfErr(err error) {
	if err != nil {
		panic(err)
	}
	return
}

func toJSON(v interface{}) string {
	out, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		panic(err)
	}
	return string(out)
}

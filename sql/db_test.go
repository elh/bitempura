package sql_test

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Masterminds/squirrel"
	bt "github.com/elh/bitempura"
	"github.com/elh/bitempura/dbtest"
	. "github.com/elh/bitempura/sql"
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	t1 = time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 = t1.AddDate(0, 0, 1)
	t3 = t1.AddDate(0, 0, 2)

	oldValue = map[string]interface{}{
		"type":      "checking",
		"balance":   0.0,
		"is_active": false,
	}
	newValue = map[string]interface{}{
		"type":      "checking",
		"balance":   100.0,
		"is_active": true,
	}
)

func TestGet(t *testing.T) {
	dbtest.TestGet(t, oldValue, newValue, func(kvs []*bt.VersionedKV) (bt.DB, func(), error) {
		sqlDB := setupTestDB(t)
		for _, kv := range kvs {
			mustInsertKV(sqlDB, "balances", "id", kv)
		}
		db, err := NewTableDB(sqlDB, "balances", "id")
		return db, closeDBFn(sqlDB), err
	})
}

func TestList(t *testing.T) {
	dbtest.TestList(t, oldValue, newValue, func(kvs []*bt.VersionedKV) (bt.DB, func(), error) {
		sqlDB := setupTestDB(t)
		for _, kv := range kvs {
			mustInsertKV(sqlDB, "balances", "id", kv)
		}
		db, err := NewTableDB(sqlDB, "balances", "id")
		return db, closeDBFn(sqlDB), err
	})
}

// func TestDelete(t *testing.T) {
// 	dbtest.TestDelete(t, oldValue, newValue, func(kvs []*bt.VersionedKV, clock bt.Clock) (db bt.DB, closeFn func(), err error) {
// 		sqlDB := setupTestDB(t)
// 		for _, kv := range kvs {
// 			mustInsertKV(sqlDB, "balances", "id", kv)
// 		}
// 		// TODO: control TX in clock...
// 		db, err = NewTableDB(sqlDB, "balances", "id")
// 		return db, closeDBFn(sqlDB), err
// 	})
// }

func TestHistory(t *testing.T) {
	dbtest.TestHistory(t, oldValue, newValue, func(kvs []*bt.VersionedKV) (bt.DB, func(), error) {
		sqlDB := setupTestDB(t)
		for _, kv := range kvs {
			mustInsertKV(sqlDB, "balances", "id", kv)
		}
		db, err := NewTableDB(sqlDB, "balances", "id")
		return db, closeDBFn(sqlDB), err
	})
}

func TestQuery(t *testing.T) {
	sqlDB := setupTestDB(t)
	defer closeDB(sqlDB)

	insert := func(id, balanceType string, balance float64, isActive bool, txTimeStart time.Time, txEndTime *time.Time,
		validTimeStart time.Time, validEndTime *time.Time) {
		mustInsertKV(sqlDB, "balances", "id", &bt.VersionedKV{
			Key: id,
			Value: map[string]interface{}{
				"type":      balanceType,
				"balance":   balance,
				"is_active": isActive,
			},
			TxTimeStart:    txTimeStart,
			TxTimeEnd:      txEndTime,
			ValidTimeStart: validTimeStart,
			ValidTimeEnd:   validEndTime,
		})
	}

	fmt.Println("alice: at t1, checking account has $100 in it and is active") // alice
	insert("alice/balance", "checking", 100, true, t1, &t3, t1, nil)
	fmt.Println("alice: at t3, balance updated to $200")
	insert("alice/balance", "checking", 100, true, t3, nil, t1, &t3)
	insert("alice/balance", "checking", 200, true, t3, nil, t3, nil)
	fmt.Println("bob: at t1, savings account has $100 and is active") // bob
	insert("bob/balance", "savings", 100, true, t1, &t2, t1, nil)
	fmt.Println("bob: at t2, realize it was $200 the entire time")
	insert("bob/balance", "savings", 300, true, t2, nil, t1, nil)
	fmt.Println("carol: at t1, checking account has $0 and is inactive") // carol
	insert("carol/balance", "checking", 0, false, t1, &t2, t1, nil)
	fmt.Println("carol: at t2, add $100 and reactivate account")
	insert("carol/balance", "checking", 0, false, t2, &t3, t1, &t2)
	insert("carol/balance", "checking", 100, true, t2, &t3, t2, nil)
	fmt.Println("carol: at t3, oh no! realized it was re-actived at t2 but amount was wrong; it was $10. it's 100 now though")
	insert("carol/balance", "checking", 10, true, t3, nil, t1, &t3)
	insert("carol/balance", "checking", 100, true, t3, nil, t3, nil)

	db, err := NewTableDB(sqlDB, "balances", "id")
	require.Nil(t, err)

	testCases := []struct {
		desc    string
		s       squirrel.SelectBuilder
		readOps []bt.ReadOpt
		expect  []map[string]interface{}
	}{
		{
			desc: "get all balance (implicitly as of TT=now, VT=now)",
			s:    squirrel.Select("*").From("balances").OrderBy("id ASC"),
			expect: []map[string]interface{}{
				{
					"__bt_id":               "NOT COMPARED", // consider hiding this. all version information?
					"__bt_tx_time_end":      nil,
					"__bt_tx_time_start":    t3,
					"__bt_valid_time_end":   nil,
					"__bt_valid_time_start": t3,
					"balance":               200.0,
					"id":                    "alice/balance",
					"is_active":             true,
					"type":                  "checking",
				},
				{
					"__bt_id":               "NOT COMPARED",
					"__bt_tx_time_end":      nil,
					"__bt_tx_time_start":    t2,
					"__bt_valid_time_end":   nil,
					"__bt_valid_time_start": t1,
					"balance":               300.0,
					"id":                    "bob/balance",
					"is_active":             true,
					"type":                  "savings",
				},
				{
					"__bt_id":               "NOT COMPARED",
					"__bt_tx_time_end":      nil,
					"__bt_tx_time_start":    t3,
					"__bt_valid_time_end":   nil,
					"__bt_valid_time_start": t3,
					"balance":               100.0,
					"id":                    "carol/balance",
					"is_active":             true,
					"type":                  "checking",
				},
			},
		},
		{
			desc: "get ids with balance > 100 at VT=t2",
			s: squirrel.Select("id", "balance").
				From("balances").
				Where(squirrel.GtOrEq{"balance": 100}).
				OrderBy("id ASC"),
			readOps: []bt.ReadOpt{bt.AsOfValidTime(t2)},
			expect: []map[string]interface{}{
				{
					"balance": 100.0,
					"id":      "alice/balance",
				},
				{
					"balance": 300.0,
					"id":      "bob/balance",
				},
			},
		},
		{
			desc: "sum of all balances as projected for t3 as known by system at t2 grouped by balance type. VT=t3, TT=t2",
			s: squirrel.Select("type", "SUM(balance)").
				From("balances").
				GroupBy("type").
				OrderBy("SUM(balance) DESC"),
			readOps: []bt.ReadOpt{bt.AsOfValidTime(t3), bt.AsOfTransactionTime(t2)},
			expect: []map[string]interface{}{
				{
					"SUM(balance)": 300.0,
					"type":         "savings",
				},
				{
					"SUM(balance)": 200.0,
					"type":         "checking",
				},
			},
		},
	}
	for _, tC := range testCases {
		tC := tC
		t.Run(tC.desc, func(t *testing.T) {
			sqlStr, _, err := tC.s.ToSql()
			require.Nil(t, err)
			fmt.Printf("query: %s\n", sqlStr)

			rows, err := db.Select(tC.s, tC.readOps...)
			require.Nil(t, err)
			defer rows.Close()

			out, err := ScanToMaps(rows)
			require.Nil(t, err)
			fmt.Println(toJSON(out))

			// can't control
			// TODO: decide if i want the base APIs to return versioning information at all
			stripBTID := func(ms []map[string]interface{}) []map[string]interface{} {
				for _, m := range ms {
					delete(m, "__bt_id")
				}
				return ms
			}

			assert.Equal(t, stripBTID(tC.expect), stripBTID(out))
		})
	}
}

// setupTestDB returns a SQLite database with a bitemporal stable table named __bt_balances_states seeded for tests.
// Caller must close the db.
func setupTestDB(t *testing.T) *sql.DB {
	file := "bitempura_test.db"
	err := os.Remove(file)
	var pathErr *os.PathError
	require.True(t, err == nil || errors.As(err, &pathErr), err)

	sqlDB, err := sql.Open("sqlite3", file)
	require.Nil(t, err)

	// set up table manually for early proof of concept check. Query is more exciting and writes are harder
	_, err = sqlDB.Exec(`
		CREATE TABLE __bt_balances_states (
			id TEXT NOT NULL, 				-- PK of the base table
			type TEXT NOT NULL,
			balance REAL NOT NULL,
			is_active BOOLEAN NOT NULL,

			__bt_id TEXT PRIMARY KEY,
			__bt_tx_time_start TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			__bt_tx_time_end TIMESTAMP NULL,
			__bt_valid_time_start TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			__bt_valid_time_end TIMESTAMP NULL
		);
	`)
	require.Nil(t, err)

	return sqlDB
}

// do not nil point exception on defer. explicitly ignore error for lint warnings
func closeDB(db *sql.DB) {
	if db != nil {
		_ = db.Close()
	}
}

// return a close function for clean up in tests
func closeDBFn(db *sql.DB) func() {
	return func() {
		closeDB(db)
	}
}

// insertKV inserts a single versioned key-value pair directly into the database.
func insertKV(db *sql.DB, tableName, pkColumnName string, kv *bt.VersionedKV) error {
	// key and time fields
	cols := []string{pkColumnName, "__bt_id", "__bt_tx_time_start", "__bt_tx_time_end", "__bt_valid_time_start", "__bt_valid_time_end"}
	vals := []interface{}{kv.Key, uuid.New().String(), kv.TxTimeStart, kv.TxTimeEnd, kv.ValidTimeStart, kv.ValidTimeEnd}

	// value
	valueMap, ok := kv.Value.(map[string]interface{})
	if !ok {
		return fmt.Errorf("value must be of type map[string]interface{}")
	}
	for k, v := range valueMap {
		cols = append(cols, k)
		vals = append(vals, v)
	}

	_, err := squirrel.
		Insert(StateTableName(tableName)).
		Columns(cols...).
		Values(vals...).
		RunWith(db).
		Exec()
	return err
}

func mustInsertKV(db *sql.DB, tableName, pkColumnName string, kv *bt.VersionedKV) {
	if err := insertKV(db, tableName, pkColumnName, kv); err != nil {
		panic(err)
	}
}

//nolint:unused,deadcode // debug
func toJSON(v interface{}) string {
	out, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		panic(err)
	}
	return string(out)
}

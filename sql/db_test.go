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
	. "github.com/elh/bitempura/sql"
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// let's get a early POC of bitemporal SQL querying
func TestQueryPOC(t *testing.T) {
	file := "bitempura_test.db"
	err := os.Remove(file)
	var pathErr *os.PathError
	require.True(t, err == nil || errors.As(err, &pathErr), err)

	sqlDB, err := sql.Open("sqlite3", file)
	defer closeDB(sqlDB)
	require.Nil(t, err)

	// set up table manually for POC check. Query is more exciting and writes are harder
	// NOTE: Oof... "Bitempur-izing" an existing table almost 100% will need to create a side table for it
	// becuase we will be taking the natural key and no longer making it a unique primary key
	_, err = sqlDB.Exec(`
		CREATE TABLE balances (
			id TEXT NOT NULL, 				-- PK of the base table
			type TEXT NOT NULL,
			balance REAL NOT NULL,
			is_active BOOLEAN NOT NULL,

			__bt_id TEXT PRIMARY KEY, 		-- dang... forgot that this definitely needs a side table because of PK
			__bt_tx_time_start TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			__bt_tx_time_end TIMESTAMP NULL,
			__bt_valid_time_start TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			__bt_valid_time_end TIMESTAMP NULL
		);
	`)
	require.Nil(t, err)

	insert := func(id, balanceType string, balance float64, isActive bool, txTimeStart time.Time, txEndTime *time.Time,
		validTimeStart time.Time, validEndTime *time.Time) {
		_, err = sqlDB.Exec(`
			INSERT INTO balances
			(
				id,
				type,
				balance,
				is_active,
				__bt_id,
				__bt_tx_time_start,
				__bt_tx_time_end,
				__bt_valid_time_start,
				__bt_valid_time_end
			)
			VALUES
			(
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?,
				?
			);
		`,
			id,
			balanceType,
			balance,
			isActive,
			uuid.New().String(),
			txTimeStart,
			txEndTime,
			validTimeStart,
			validEndTime,
		)
		require.Nil(t, err)
	}

	t1 := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := t1.AddDate(0, 0, 1)
	t3 := t1.AddDate(0, 0, 2)
	insert("alice/balance", "checking", 100, true, t1, &t3, t1, nil) // alice: checking 100 active
	insert("alice/balance", "checking", 100, true, t3, nil, t1, &t3) // at t3, updated account to 200
	insert("alice/balance", "checking", 200, true, t3, nil, t3, nil) //
	insert("bob/balance", "savings", 100, true, t1, &t2, t1, nil)    // bob: savings 100 active
	insert("bob/balance", "savings", 300, true, t2, nil, t1, nil)    // at t2, realize it was 200 the entire time
	insert("carol/balance", "checking", 0, false, t1, &t2, t1, nil)  // carol: checking 0 inactive
	insert("carol/balance", "checking", 0, false, t2, &t3, t1, &t2)  // at t2, add 100, reactivate account
	insert("carol/balance", "checking", 100, true, t2, &t3, t2, nil) //
	insert("carol/balance", "checking", 10, true, t3, nil, t1, &t3)  // at t3, oh no. realized it was re-actived but amount was wrong. it's 100 now
	insert("carol/balance", "checking", 100, true, t3, nil, t3, nil) //

	tableName := "balances"
	db, err := NewTableDB(sqlDB, tableName, "id")
	require.Nil(t, err)

	// -----------------------------------------------------------------------------------------------------------------
	// get all balance (implicitly as of TT=now, VT=now)
	fmt.Println("get all balance (implicitly as of TT=now, VT=now)")
	_ = db
	s := squirrel.Select("*").From("balances").OrderBy("id ASC")
	rows, err := db.Select(s)
	require.Nil(t, err)
	defer rows.Close()

	out, err := scanToMaps(rows)
	require.Nil(t, err)
	fmt.Println(toJSON(out))

	require.Len(t, out, 3)
	assert.Equal(t, "alice/balance", out[0]["id"])
	assert.Equal(t, 200.0, out[0]["balance"])
	assert.Equal(t, "bob/balance", out[1]["id"])
	assert.Equal(t, 300.0, out[1]["balance"])
	assert.Equal(t, "carol/balance", out[2]["id"])
	assert.Equal(t, 100.0, out[2]["balance"])

	// -----------------------------------------------------------------------------------------------------------------
	// get ids of all checking accounts with balance >= 100 at VT=t2
	fmt.Println("get ids with balance > 100 at VT=t2")
	_ = db
	s = squirrel.Select("id", "balance").
		From("balances").
		Where(squirrel.GtOrEq{"balance": 100}).
		OrderBy("id ASC")
	rows, err = db.Select(s, bt.AsOfValidTime(t2))
	require.Nil(t, err)
	defer rows.Close()

	out, err = scanToMaps(rows)
	require.Nil(t, err)
	fmt.Println(toJSON(out))

	require.Len(t, out, 2)
	assert.Equal(t, "alice/balance", out[0]["id"])
	assert.Equal(t, 100.0, out[0]["balance"])
	assert.Equal(t, "bob/balance", out[1]["id"])
	assert.Equal(t, 300.0, out[1]["balance"])

	// -----------------------------------------------------------------------------------------------------------------
	// sum of all balances as projected for t3 as known by system at t2 grouped by balance type. VT=t3, TT=t2
	fmt.Println("sum of all balances as projected for t3 as known by system at t2 grouped by balance type. VT=t3, TT=t2")
	s = squirrel.Select("type", "SUM(balance)").
		From("balances").
		GroupBy("type").
		OrderBy("SUM(balance) DESC")
	rows, err = db.Select(s, bt.AsOfValidTime(t3), bt.AsOfTransactionTime(t2))
	require.Nil(t, err)
	defer rows.Close()

	out, err = scanToMaps(rows)
	require.Nil(t, err)
	fmt.Println(toJSON(out))

	require.Len(t, out, 2)
	assert.Equal(t, "savings", out[0]["type"])
	assert.Equal(t, 300.0, out[0]["SUM(balance)"])
	assert.Equal(t, "checking", out[1]["type"])
	assert.Equal(t, 200.0, out[1]["SUM(balance)"])
}

// scanToMap generically scans SQL rows into a slice of maps with columns as map keys
// caller should defer rows.Close() but does not need to call rows.Err()
func scanToMaps(rows *sql.Rows) ([]map[string]interface{}, error) {
	var out []map[string]interface{}

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		rowMap, err := scanToMap(rows, cols)
		if err != nil {
			return nil, err
		}
		out = append(out, rowMap)
	}
	if err = rows.Err(); err != nil {
		panic(err)
	}
	return out, nil
}

func scanToMap(row *sql.Rows, cols []string) (map[string]interface{}, error) {
	fields := make([]interface{}, len(cols))
	fieldPtrs := make([]interface{}, len(cols))
	for i := range fields {
		fieldPtrs[i] = &fields[i]
	}

	if err := row.Scan(fieldPtrs...); err != nil {
		return nil, err
	}

	out := map[string]interface{}{}
	for i, col := range cols {
		out[col] = fields[i]
	}
	return out, nil
}

// do not nil point exception on defer
func closeDB(db *sql.DB) {
	if db != nil {
		_ = db.Close()
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

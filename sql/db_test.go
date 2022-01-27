package sql_test

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	. "github.com/elh/bitempura/sql"
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
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

	insert := func(id, balanceType string, balance int, isActive bool, txTimeStart time.Time, txEndTime *time.Time,
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
	insert("bob/balance", "savings", 200, true, t2, nil, t1, nil)    // at t2, realize it was 200 the entire time
	insert("carol/balance", "checking", 0, false, t1, &t2, t1, nil)  // carol: checking 0 inactive
	insert("carol/balance", "checking", 0, false, t2, &t3, t1, &t2)  // at t2, add 100, reactivate account
	insert("carol/balance", "checking", 100, true, t2, &t3, t2, nil) //
	insert("carol/balance", "checking", 10, true, t3, nil, t1, &t3)  // at t3, oh no. realized it was re-actived but amount was wrong. it's 100 now
	insert("carol/balance", "checking", 100, true, t3, nil, t3, nil) //

	tableName := "balances"
	db, err := NewTableDB(sqlDB, tableName, "id")
	require.Nil(t, err)

	_ = db
	// s := squirrel.Select("*").From("balances")
	// rows, err := db.Select(s)

	rows, err := sqlDB.Query("SELECT * FROM balances")
	require.Nil(t, err)
	defer rows.Close()

	var out []map[string]interface{}

	cols, err := rows.Columns()
	require.Nil(t, err)

	for rows.Next() {
		rowMap, err := scanToMap(rows, cols)
		require.Nil(t, err)
		out = append(out, rowMap)
	}
	if err = rows.Err(); err != nil {
		panic(err)
	}

	fmt.Println(toJSON(out))
}

// scanToMap scans a SQL row with a dynamic list of columns into a map
func scanToMap(row *sql.Rows, cols []string) (map[string]interface{}, error) {
	out := map[string]interface{}{}
	fields := make([]interface{}, len(cols))
	fieldPtrs := make([]interface{}, len(cols))
	for i := range fields {
		fieldPtrs[i] = &fields[i]
	}
	for i, col := range cols {
		out[col] = &fieldPtrs[i]
	}

	if err := row.Scan(fieldPtrs...); err != nil {
		return nil, err
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

package sql

import (
	"database/sql"
	"errors"
	"time"

	"github.com/Masterminds/squirrel"
	bt "github.com/elh/bitempura"
)

var _ DB = (*TableDB)(nil)

// DB is a SQL-backed, SQL-queryable, bitemporal database.
// WARNING: WIP. this implementation is experimental.
type DB interface {
	bt.DB
	// Select executes a SQL query (as of optional valid and transaction times).
	Select(query squirrel.SelectBuilder, opts ...bt.ReadOpt) (*sql.Rows, error)
}

// NewTableDB constructs a SQL-backed, SQL-queryable, bitemporal database connected to a specific underlying SQL table.
// WARNING: WIP. this implementation is experimental.
func NewTableDB(eq ExecerQueryer, table string, pkColumnName string) (DB, error) {
	// TODO: support composite PK through a pkFn(key string) Key struct
	return &TableDB{
		eq:           eq,
		table:        table,
		pkColumnName: pkColumnName,
	}, nil
}

// TableDB is a SQL-backed, SQL-queryable, bitemporal database that is connected to a specific underlying SQL table.
type TableDB struct {
	eq           ExecerQueryer
	table        string
	pkColumnName string
}

// Get data by key (as of optional valid and transaction times).
// WARNING: unimplemented
func (db *TableDB) Get(key string, opts ...bt.ReadOpt) (*bt.VersionedKV, error) {
	// SELECT *
	// FROM <table>
	// WHERE
	// 		<base table pk> = <key> AND
	//		__bt_tx_time_start <= <as_of_tx_time> AND
	//		(__bt_tx_time_end IS NULL OR __bt_tx_time_end > <as_of_tx_time>) AND
	//		__bt_valid_time_start <= <as_of_valid_time> AND
	//		(__bt_valid_time_end IS NULL OR __bt_valid_time_end > <as_of_valid_time>)
	// LIMIT 1
	return nil, errors.New("unimplemented")
}

// List all data (as of optional valid and transaction times).
// WARNING: unimplemented
func (db *TableDB) List(opts ...bt.ReadOpt) ([]*bt.VersionedKV, error) {
	// SELECT *
	// FROM <table>
	// WHERE
	// 		<base table pk> = <key> AND
	//		__bt_tx_time_start <= <as_of_tx_time> AND
	//		(__bt_tx_time_end IS NULL OR __bt_tx_time_end > <as_of_tx_time>) AND
	//		__bt_valid_time_start <= <as_of_valid_time> AND
	//		(__bt_valid_time_end IS NULL OR __bt_valid_time_end > <as_of_valid_time>)
	return nil, errors.New("unimplemented")
}

// Set stores value (with optional start and end valid time).
// WARNING: unimplemented
func (db *TableDB) Set(key string, value bt.Value, opts ...bt.WriteOpt) error {
	// INSERT
	// INTO <table>
	// (<fields...>, __bt_tx_time_start, __bt_tx_time_end, __bt_valid_time_start, __bt_valid_time_end)
	// VALUES
	// (<values...>, <tx_time_start>, <tx_time_end>, <valid_time_start>, <valid_time_end>)
	//
	// select out the conflicting records based on the write opt times. update them and add new ones as needed
	return errors.New("unimplemented")
}

// Delete removes value (with optional start and end valid time).
// WARNING: unimplemented
func (db *TableDB) Delete(key string, opts ...bt.WriteOpt) error {
	// select out the conflicting records based on the write opt times. update them and add new ones as needed
	return errors.New("unimplemented")
}

// History returns versions by descending end transaction time, descending end valid time
// WARNING: unimplemented
func (db *TableDB) History(key string) ([]*bt.VersionedKV, error) {
	// SELECT *
	// FROM <table>
	// WHERE
	// 		<base table pk> = <key>
	return nil, errors.New("unimplemented")
}

// Select executes a SQL query (as of optional valid and transaction times).
func (db *TableDB) Select(b squirrel.SelectBuilder, opts ...bt.ReadOpt) (*sql.Rows, error) {
	options := db.handleReadOpts(opts)

	// add tx and valid time to query
	b = b.Where(squirrel.LtOrEq{"__bt_tx_time_start": options.TxTime})
	b = b.Where(squirrel.Or{squirrel.Eq{"__bt_tx_time_end": nil}, squirrel.Gt{"__bt_tx_time_end": options.TxTime}})
	b = b.Where(squirrel.LtOrEq{"__bt_valid_time_start": options.ValidTime})
	b = b.Where(squirrel.Or{squirrel.Eq{"__bt_valid_time_end": nil}, squirrel.Gt{"__bt_valid_time_end": options.ValidTime}})

	return b.RunWith(db.eq).Query()
}

func (db *TableDB) handleReadOpts(opts []bt.ReadOpt) *bt.ReadOptions {
	now := time.Now()
	options := &bt.ReadOptions{
		ValidTime: now,
		TxTime:    now,
	}
	for _, opt := range opts {
		opt(options)
	}

	return options
}

// ExecerQueryer can Exec or Query. Both sql.DB and sql.Tx satisfy this interface.
type ExecerQueryer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

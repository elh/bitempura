package sql

import (
	"database/sql"
	"errors"

	"github.com/Masterminds/squirrel"
	bt "github.com/elh/bitempura"
)

var _ DB = (*TableDB)(nil)

// DB is a SQL-backed, SQL-queryable, bitemporal database.
type DB interface {
	bt.DB
	// Select executes a SQL query (as of optional valid and transaction times).
	Select(query squirrel.SelectBuilder, opts ...bt.ReadOpt) (*sql.Rows, error)
}

// NewTableDB constructs a SQL-backed, SQL-queryable, bitemporal database connected to a specific underlying SQL table.
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
func (db *TableDB) Get(key string, opts ...bt.ReadOpt) (*bt.VersionedKV, error) {
	// SELECT *
	// FROM <table>
	// WHERE
	// 		<base table pk> = <key> AND
	//		$tx_time_start <= <as_of_tx_time> AND
	//		($tx_time_end IS NULL OR $tx_time_end > <as_of_tx_time>) AND
	//		$valid_time_start <= <as_of_valid_time> AND
	//		($valid_time_end IS NULL OR $valid_time_end > <as_of_valid_time>)
	// LIMIT 1
	return nil, errors.New("unimplemented")
}

// List all data (as of optional valid and transaction times).
func (db *TableDB) List(opts ...bt.ReadOpt) ([]*bt.VersionedKV, error) {
	// SELECT *
	// FROM <table>
	// WHERE
	// 		<base table pk> = <key> AND
	//		$tx_time_start <= <as_of_tx_time> AND
	//		($tx_time_end IS NULL OR $tx_time_end > <as_of_tx_time>) AND
	//		$valid_time_start <= <as_of_valid_time> AND
	//		($valid_time_end IS NULL OR $valid_time_end > <as_of_valid_time>)
	return nil, errors.New("unimplemented")
}

// Set stores value (with optional start and end valid time).
func (db *TableDB) Set(key string, value bt.Value, opts ...bt.WriteOpt) error {
	// INSERT
	// INTO <table>
	// (<fields...>, $tx_time_start, $tx_time_end, $valid_time_start, $valid_time_end)
	// VALUES
	// (<values...>, <tx_time_start>, <tx_time_end>, <valid_time_start>, <valid_time_end>)
	//
	// select out the conflicting records based on the write opt times. update them and add new ones as needed
	return errors.New("unimplemented")
}

// Delete removes value (with optional start and end valid time).
func (db *TableDB) Delete(key string, opts ...bt.WriteOpt) error {
	// select out the conflicting records based on the write opt times. update them and add new ones as needed
	return errors.New("unimplemented")
}

// History returns versions by descending end transaction time, descending end valid time
func (db *TableDB) History(key string) ([]*bt.VersionedKV, error) {
	// SELECT *
	// FROM <table>
	// WHERE
	// 		<base table pk> = <key>
	return nil, errors.New("unimplemented")
}

// Select executes a SQL query (as of optional valid and transaction times).
func (db *TableDB) Select(b squirrel.SelectBuilder, opts ...bt.ReadOpt) (*sql.Rows, error) {
	return nil, errors.New("unimplemented")
}

// ExecerQueryer can Exec or Query. Both sql.DB and sql.Tx satisfy this interface.
type ExecerQueryer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

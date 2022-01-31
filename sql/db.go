package sql

import (
	"database/sql"
	"errors"
	"fmt"
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

// StateTableName returns the default bitemporal state table name for a given table.
func StateTableName(tableName string) string {
	return fmt.Sprintf("__bt_%v_states", tableName)
}

// NewTableDB constructs a SQL-backed, SQL-queryable, bitemporal database connected to a specific underlying SQL table.
// WARNING: WIP. this implementation is experimental.
func NewTableDB(eq ExecerQueryer, table string, pkColumnName string, updatedAtColName,
	deletedAtColName *string) (DB, error) {
	// TODO: convert UpdateAt and DeletedAt columns to options
	// TODO: support composite PK through a pkFn(key string) Key struct
	return &TableDB{
		eq:               eq,
		table:            table,
		stateTable:       StateTableName(table),
		pkColumnName:     pkColumnName,
		updatedAtColName: updatedAtColName,
		deletedAtColName: deletedAtColName,
	}, nil
}

// TableDB is a SQL-backed, SQL-queryable, bitemporal database that is connected to a specific underlying SQL table.
type TableDB struct {
	eq               ExecerQueryer
	table            string
	stateTable       string
	pkColumnName     string
	updatedAtColName *string
	deletedAtColName *string
}

// Get data by key (as of optional valid and transaction times).
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
	b := squirrel.Select("*").
		From(db.stateTable).
		Where(squirrel.Eq{db.pkColumnName: key}).
		Limit(1)
	rows, err := db.Select(b, opts...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	kvs, err := ScanToVersionedKVs(db.pkColumnName, rows)
	if err != nil {
		return nil, err
	}
	if len(kvs) == 0 {
		return nil, bt.ErrNotFound
	}
	return kvs[0], nil
}

// List all data (as of optional valid and transaction times).
func (db *TableDB) List(opts ...bt.ReadOpt) ([]*bt.VersionedKV, error) {
	// SELECT *
	// FROM <table>
	// WHERE
	// 		<base table pk> = <key> AND
	//		__bt_tx_time_start <= <as_of_tx_time> AND
	//		(__bt_tx_time_end IS NULL OR __bt_tx_time_end > <as_of_tx_time>) AND
	//		__bt_valid_time_start <= <as_of_valid_time> AND
	//		(__bt_valid_time_end IS NULL OR __bt_valid_time_end > <as_of_valid_time>)
	b := squirrel.Select("*").
		From(db.stateTable)
	rows, err := db.Select(b, opts...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	kvs, err := ScanToVersionedKVs(db.pkColumnName, rows)
	if err != nil {
		return nil, err
	}
	return kvs, nil
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
	writeConfig, now, err := db.handleWriteOpts(opts)
	if err != nil {
		return err
	}

	// select out the conflicting records based on the write opt times. update them and add new ones as needed
	if db.deletedAtColName == nil {
		return errors.New("Delete without configured DeleteAt column is unimplemented") // TODO: support this
	}
	if writeConfig.endValidTime != nil && writeConfig.endValidTime.Before(now) {
		return errors.New("Delete in the past not supported") // TODO: support this
	}

	res, err := squirrel.Update(db.table).
		Set(*db.deletedAtColName, now).
		Where(squirrel.Eq{db.pkColumnName: key}).
		RunWith(db.eq).
		Exec()
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return bt.ErrNotFound // TODO: check before delete. cannot assume safe rollback.
	}

	return nil
}

// History returns versions by descending end transaction time, descending end valid time
func (db *TableDB) History(key string) ([]*bt.VersionedKV, error) {
	// SELECT *
	// FROM <table>
	// WHERE
	// 		<base table pk> = <key>
	// ORDER BY __bt_tx_time_end DESC, __bt_valid_time_end DESC
	rows, err := squirrel.Select("*").
		From(db.stateTable).
		Where(squirrel.Eq{db.pkColumnName: key}).
		OrderBy("__bt_tx_time_end IS NULL DESC, __bt_tx_time_end DESC, __bt_valid_time_end IS NULL DESC, __bt_valid_time_end DESC").
		RunWith(db.eq).
		Query()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	kvs, err := ScanToVersionedKVs(db.pkColumnName, rows)
	if err != nil {
		return nil, err
	}
	if len(kvs) == 0 {
		return nil, bt.ErrNotFound
	}
	return kvs, nil
}

// Select executes a SQL query (as of optional valid and transaction times).
func (db *TableDB) Select(b squirrel.SelectBuilder, opts ...bt.ReadOpt) (*sql.Rows, error) {
	options := db.handleReadOpts(opts)

	// override FROM table
	b = b.From(db.stateTable)
	// add tx and valid time to query
	b = b.Where(squirrel.LtOrEq{"__bt_tx_time_start": options.txTime})
	b = b.Where(squirrel.Or{squirrel.Eq{"__bt_tx_time_end": nil}, squirrel.Gt{"__bt_tx_time_end": options.txTime}})
	b = b.Where(squirrel.LtOrEq{"__bt_valid_time_start": options.validTime})
	b = b.Where(squirrel.Or{squirrel.Eq{"__bt_valid_time_end": nil}, squirrel.Gt{"__bt_valid_time_end": options.validTime}})

	return b.RunWith(db.eq).Query()
}

type readConfig struct {
	validTime time.Time
	txTime    time.Time
}

func (db *TableDB) handleReadOpts(opts []bt.ReadOpt) *readConfig {
	options := bt.ApplyReadOpts(opts)

	now := time.Now()
	config := &readConfig{
		validTime: now,
		txTime:    now,
	}
	if options.ValidTime != nil {
		config.validTime = *options.ValidTime
	}
	if options.TxTime != nil {
		config.txTime = *options.TxTime
	}

	return config
}

type writeConfig struct {
	validTime    time.Time
	endValidTime *time.Time
}

func (db *TableDB) handleWriteOpts(opts []bt.WriteOpt) (config *writeConfig, now time.Time, err error) {
	options := bt.ApplyWriteOpts(opts)

	now = time.Now()
	config = &writeConfig{
		validTime:    now,
		endValidTime: nil,
	}
	if options.ValidTime != nil {
		config.validTime = *options.ValidTime
	}
	if options.EndValidTime != nil {
		config.endValidTime = options.EndValidTime
	}

	// validate write option times. this is relevant for Delete even if Set is validated at resource level
	if config.endValidTime != nil && !config.endValidTime.After(config.validTime) {
		return nil, time.Time{}, errors.New("valid time start must be before end")
	}
	// disallow valid times being set in the future
	if config.validTime.After(now) {
		return nil, time.Time{}, errors.New("valid time start cannot be in the future")
	}
	if config.endValidTime != nil && config.endValidTime.After(now) {
		return nil, time.Time{}, errors.New("valid time end cannot be in the future")
	}

	return config, now, nil
}

// ExecerQueryer can Exec or Query. Both sql.DB and sql.Tx satisfy this interface.
type ExecerQueryer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

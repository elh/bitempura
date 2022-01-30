package sql

import (
	"database/sql"
	"fmt"
	"time"

	bt "github.com/elh/bitempura"
)

// ScanToVersionedKVs generically scans SQL rows into a slice of VersionedKV's. Caller should defer rows.Close() but
// does not need to call rows.Err()
func ScanToVersionedKVs(pkColumnName string, rows *sql.Rows) ([]*bt.VersionedKV, error) {
	maps, err := ScanToMaps(rows)
	if err != nil {
		return nil, err
	}

	out := make([]*bt.VersionedKV, len(maps))
	for i, m := range maps {
		keyI, ok := m[pkColumnName]
		if !ok {
			return nil, fmt.Errorf("missing pk column %s", pkColumnName)
		}
		key, ok := keyI.(string)
		if !ok {
			return nil, fmt.Errorf("key is not of type string")
		}

		txTimeStartI, ok := m["__bt_tx_time_start"]
		if !ok {
			return nil, fmt.Errorf("missing __bt_tx_time_start column")
		}
		txTimeStart, ok := txTimeStartI.(time.Time)
		if !ok {
			return nil, fmt.Errorf("__bt_tx_time_start value is not of type time.Time")
		}
		var txTimeEnd time.Time
		txTimeEndI, ok := m["__bt_tx_time_end"]
		if !ok {
			return nil, fmt.Errorf("missing __bt_tx_time_end column")
		}
		if txTimeEndI != nil {
			txTimeEnd, ok = txTimeEndI.(time.Time)
			if !ok {
				return nil, fmt.Errorf("__bt_tx_time_end value is not of type *time.Time (sql.NullTime)")
			}
		}

		validTimeStartI, ok := m["__bt_valid_time_start"]
		if !ok {
			return nil, fmt.Errorf("missing __bt_valid_time_start column")
		}
		validTimeStart, ok := validTimeStartI.(time.Time)
		if !ok {
			return nil, fmt.Errorf("__bt_valid_time_start value is not of type time.Time")
		}
		var validTimeEnd time.Time
		validTimeEndI, ok := m["__bt_valid_time_end"]
		if !ok {
			return nil, fmt.Errorf("missing __bt_valid_time_end column")
		}
		if validTimeEndI != nil {
			validTimeEnd, ok = validTimeEndI.(time.Time)
			if !ok {
				return nil, fmt.Errorf("__bt_valid_time_end value is not of type *time.Time (sql.NullTime)")
			}
		}

		val := map[string]interface{}{}
		for k, v := range m {
			if k != pkColumnName &&
				k != "__bt_id" &&
				k != "__bt_tx_time_start" &&
				k != "__bt_tx_time_end" &&
				k != "__bt_valid_time_start" &&
				k != "__bt_valid_time_end" {
				val[k] = v
			}
		}
		kv := &bt.VersionedKV{
			Key:            key,
			Value:          val,
			TxTimeStart:    txTimeStart,
			TxTimeEnd:      toTimePtr(txTimeEnd),
			ValidTimeStart: validTimeStart,
			ValidTimeEnd:   toTimePtr(validTimeEnd),
		}
		out[i] = kv
	}
	return out, nil
}

func toTimePtr(t time.Time) *time.Time {
	return &t
}

// ScanToMaps generically scans SQL rows into a slice of maps with columns as map keys. Caller should defer
// rows.Close() but does not need to call rows.Err()
func ScanToMaps(rows *sql.Rows) ([]map[string]interface{}, error) {
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

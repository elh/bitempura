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
		key, err := getString(pkColumnName, m)
		if err != nil {
			return nil, err
		}
		txTimeStart, err := getTime("__bt_tx_time_start", m)
		if err != nil {
			return nil, err
		}
		txTimeEnd, err := getNullTime("__bt_tx_time_end", m)
		if err != nil {
			return nil, err
		}
		validTimeStart, err := getTime("__bt_valid_time_start", m)
		if err != nil {
			return nil, err
		}
		validTimeEnd, err := getNullTime("__bt_valid_time_end", m)
		if err != nil {
			return nil, err
		}

		val := map[string]interface{}{}
		for k, v := range m {
			if k != pkColumnName && k != "__bt_id" && k != "__bt_tx_time_start" && k != "__bt_tx_time_end" &&
				k != "__bt_valid_time_start" && k != "__bt_valid_time_end" {
				val[k] = v
			}
		}
		kv := &bt.VersionedKV{
			Key:            key,
			Value:          val,
			TxTimeStart:    txTimeStart,
			TxTimeEnd:      txTimeEnd,
			ValidTimeStart: validTimeStart,
			ValidTimeEnd:   validTimeEnd,
		}
		out[i] = kv
	}
	return out, nil
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

func getString(key string, m map[string]interface{}) (string, error) {
	v, ok := m[key]
	if !ok {
		return "", fmt.Errorf("missing key %s", key)
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("value for key %s is not of type string", key)
	}
	return s, nil
}

func getTime(key string, m map[string]interface{}) (time.Time, error) {
	v, ok := m[key]
	if !ok {
		return time.Time{}, fmt.Errorf("missing key %s", key)
	}
	t, ok := v.(time.Time)
	if !ok {
		return time.Time{}, fmt.Errorf("value for key %s is not of type time.Time", key)
	}
	return t, nil
}

// due to handling by ScanToMaps, value will either be nil or of type time.Time
func getNullTime(key string, m map[string]interface{}) (*time.Time, error) {
	v, ok := m[key]
	if !ok {
		return nil, fmt.Errorf("missing key %s", key)
	}
	if v == nil {
		return nil, nil
	}
	t, ok := v.(time.Time)
	if !ok {
		return nil, fmt.Errorf("value for key %s is not of type time.Time", key)
	}
	return &t, nil
}

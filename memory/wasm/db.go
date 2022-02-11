//go:build js && wasm
// +build js,wasm

package wasm

import (
	"encoding/json"
	"fmt"
	"syscall/js"
	"time"

	"github.com/elh/bitempura"
	bt "github.com/elh/bitempura"
	"github.com/elh/bitempura/dbtest"
	"github.com/elh/bitempura/memory"
)

var clock *dbtest.TestClock
var db bitempura.DB

func init() {
	var err error
	clock = &dbtest.TestClock{}
	db, err = memory.NewDB(memory.WithClock(clock))
	if err != nil {
		fmt.Printf("ERROR: failed to init db: %v\n", err)
		return
	}
	_ = db

	// TODO: remove this pre-seeding for testing
	if err := clock.SetNow(time.Date(2022, time.January, 1, 0, 0, 0, 0, time.UTC)); err != nil {
		fmt.Printf("ERROR: failed to set up clock: %v\n", err)
		return
	}
	if err := db.Set("a", 1); err != nil {
		fmt.Printf("ERROR: failed to pre-seed db: %v\n", err)
		return
	}
	if err := db.Set("b", 2); err != nil {
		fmt.Printf("ERROR: failed to pre-seed db: %v\n", err)
		return
	}

	fmt.Println("bt db initialized.")
}

// Get is the wasm adapter for DB.Get.
// arguments: key: string, [as_of_valid_time: datetime (as RFC 3339 string), as_of_transaction_time: datetime (as RFC 3339 string)]
func Get(this js.Value, inputs []js.Value) interface{} {
	res, err := get(inputs)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
	}
	return res
}

func get(inputs []js.Value) (interface{}, error) {
	var key string
	var asOfValidTime, asOfTransactionTime *time.Time
	{
		if len(inputs) < 1 {
			return nil, fmt.Errorf("key is required")
		}
		if inputs[0].Type() != js.TypeString {
			return nil, fmt.Errorf("key must be type string")
		}
		key = inputs[0].String()
	}
	if len(inputs) > 1 && inputs[1].Type() != js.TypeNull && inputs[1].Type() != js.TypeUndefined {
		if inputs[1].Type() != js.TypeString {
			return nil, fmt.Errorf("as_of_valid_time must be type string (or null or undefined)")
		}
		t, err := time.Parse(time.RFC3339, inputs[1].String())
		if err != nil {
			return nil, fmt.Errorf("failed to parse as_of_valid_time: %v\n", err)
		}
		asOfValidTime = &t
	}
	if len(inputs) > 2 && inputs[2].Type() != js.TypeNull && inputs[2].Type() != js.TypeUndefined {
		if inputs[2].Type() != js.TypeString {
			return nil, fmt.Errorf("as_of_transaction_time must be type string (or null or undefined)")
		}
		t, err := time.Parse(time.RFC3339, inputs[2].String())
		if err != nil {
			return nil, fmt.Errorf("failed to parse as_of_transaction_time: %v\n", err)
		}
		asOfTransactionTime = &t
	}

	var opts []bt.ReadOpt
	if asOfValidTime != nil {
		opts = append(opts, bt.AsOfValidTime(*asOfValidTime))
	}
	if asOfTransactionTime != nil {
		opts = append(opts, bt.AsOfTransactionTime(*asOfTransactionTime))
	}
	got, err := db.Get(key, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to get: %v\n", err)
	}
	res, err := kvToMap(got)
	if err != nil {
		return nil, fmt.Errorf("failed to convert kv: %v\n", err)
	}
	return res, nil
}

// List is the wasm adapter for DB.List
// arguments: [as_of_valid_time: datetime (as RFC 3339 string), as_of_transaction_time: datetime (as RFC 3339 string)]
func List(this js.Value, inputs []js.Value) interface{} {
	res, err := list(inputs)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
	}
	return res
}

func list(inputs []js.Value) (interface{}, error) {
	var asOfValidTime, asOfTransactionTime *time.Time
	if len(inputs) > 0 && inputs[0].Type() != js.TypeNull && inputs[0].Type() != js.TypeUndefined {
		if inputs[0].Type() != js.TypeString {
			return nil, fmt.Errorf("as_of_valid_time must be type string (or null or undefined)")
		}
		t, err := time.Parse(time.RFC3339, inputs[0].String())
		if err != nil {
			return nil, fmt.Errorf("failed to parse as_of_valid_time: %v\n", err)
		}
		asOfValidTime = &t
	}
	if len(inputs) > 1 && inputs[1].Type() != js.TypeNull && inputs[1].Type() != js.TypeUndefined {
		if inputs[1].Type() != js.TypeString {
			return nil, fmt.Errorf("as_of_transaction_time must be type string (or null or undefined)")
		}
		t, err := time.Parse(time.RFC3339, inputs[1].String())
		if err != nil {
			return nil, fmt.Errorf("failed to parse as_of_transaction_time: %v\n", err)
		}
		asOfTransactionTime = &t
	}

	var opts []bt.ReadOpt
	if asOfValidTime != nil {
		opts = append(opts, bt.AsOfValidTime(*asOfValidTime))
	}
	if asOfTransactionTime != nil {
		opts = append(opts, bt.AsOfTransactionTime(*asOfTransactionTime))
	}
	got, err := db.List(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to list: %v\n", err)
	}
	res, err := kvsToSlice(got)
	if err != nil {
		return nil, fmt.Errorf("failed to convert kvs: %v\n", err)
	}
	return res, nil
}

// Set is the wasm adapter for DB.Set
// arguments: key: string, value: string (JSON string), [with_valid_time: datetime (as RFC 3339 string), with_end_valid)time: datetime (as RFC 3339 string)]
func Set(this js.Value, inputs []js.Value) interface{} {
	err := set(inputs)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
	}
	return nil
}

func set(inputs []js.Value) error {
	var key, value string
	var withValidTime, withEndValidTime *time.Time
	{
		if len(inputs) < 1 {
			return fmt.Errorf("key is required")
		}
		if inputs[0].Type() != js.TypeString {
			return fmt.Errorf("key must be type string")
		}
		key = inputs[0].String()
	}
	{
		if len(inputs) < 2 {
			return fmt.Errorf("value is required")
		}
		if inputs[1].Type() != js.TypeString {
			return fmt.Errorf("key must be type string")
		}
		value = inputs[1].String()
	}
	if len(inputs) > 2 && inputs[2].Type() != js.TypeNull && inputs[2].Type() != js.TypeUndefined {
		if inputs[2].Type() != js.TypeString {
			return fmt.Errorf("with_valid_time must be type string (or null or undefined)")
		}
		t, err := time.Parse(time.RFC3339, inputs[2].String())
		if err != nil {
			return fmt.Errorf("failed to parse with_valid_time: %v\n", err)
		}
		withValidTime = &t
	}
	if len(inputs) > 3 && inputs[3].Type() != js.TypeNull && inputs[3].Type() != js.TypeUndefined {
		if inputs[3].Type() != js.TypeString {
			return fmt.Errorf("with_end_valid must be type string (or null or undefined)")
		}
		t, err := time.Parse(time.RFC3339, inputs[3].String())
		if err != nil {
			return fmt.Errorf("failed to parse with_end_valid: %v\n", err)
		}
		withEndValidTime = &t
	}

	var opts []bt.WriteOpt
	if withValidTime != nil {
		opts = append(opts, bt.WithValidTime(*withValidTime))
	}
	if withEndValidTime != nil {
		opts = append(opts, bt.WithEndValidTime(*withEndValidTime))
	}
	err := db.Set(key, value, opts...)
	if err != nil {
		return fmt.Errorf("failed to set: %v\n", err)
	}
	return nil
}

// Delete is the wasm adapter for DB.Delete
// arguments: key: string, [with_valid_time: datetime (as RFC 3339 string), with_end_valid)time: datetime (as RFC 3339 string)]
func Delete(this js.Value, inputs []js.Value) interface{} {
	err := delete(inputs)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
	}
	return nil
}

func delete(inputs []js.Value) error {
	var key string
	var withValidTime, withEndValidTime *time.Time
	{
		if len(inputs) < 1 {
			return fmt.Errorf("key is required")
		}
		if inputs[0].Type() != js.TypeString {
			return fmt.Errorf("key must be type string")
		}
		key = inputs[0].String()
	}
	if len(inputs) > 1 && inputs[1].Type() != js.TypeNull && inputs[1].Type() != js.TypeUndefined {
		if inputs[1].Type() != js.TypeString {
			return fmt.Errorf("with_valid_time must be type string (or null or undefined)")
		}
		t, err := time.Parse(time.RFC3339, inputs[1].String())
		if err != nil {
			return fmt.Errorf("failed to parse with_valid_time: %v\n", err)
		}
		withValidTime = &t
	}
	if len(inputs) > 2 && inputs[2].Type() != js.TypeNull && inputs[2].Type() != js.TypeUndefined {
		if inputs[2].Type() != js.TypeString {
			return fmt.Errorf("with_end_valid must be type string (or null or undefined)")
		}
		t, err := time.Parse(time.RFC3339, inputs[2].String())
		if err != nil {
			return fmt.Errorf("failed to parse with_end_valid: %v\n", err)
		}
		withEndValidTime = &t
	}

	var opts []bt.WriteOpt
	if withValidTime != nil {
		opts = append(opts, bt.WithValidTime(*withValidTime))
	}
	if withEndValidTime != nil {
		opts = append(opts, bt.WithEndValidTime(*withEndValidTime))
	}
	err := db.Delete(key, opts...)
	if err != nil {
		return fmt.Errorf("failed to delete: %v\n", err)
	}
	return nil
}

// History is the wasm adapter for DB.History
// arguments: key: string
func History(this js.Value, inputs []js.Value) interface{} {
	res, err := history(inputs)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
	}
	return res
}

func history(inputs []js.Value) (interface{}, error) {
	var key string
	{
		if len(inputs) < 1 {
			return nil, fmt.Errorf("key is required")
		}
		if inputs[0].Type() != js.TypeString {
			return nil, fmt.Errorf("key must be type string")
		}
		key = inputs[0].String()
	}

	got, err := db.History(key)
	if err != nil {
		return nil, fmt.Errorf("failed to get history: %v\n", err)
	}
	res, err := kvsToSlice(got)
	if err != nil {
		return nil, fmt.Errorf("failed to convert kvs: %v\n", err)
	}
	return res, nil
}

// SetNow is the wasm adapter for dbtest.TestClock.SetNow
func SetNow(this js.Value, inputs []js.Value) interface{} {
	fmt.Println("unimplemented")
	return nil
}

func kvsToSlice(kvs []*bt.VersionedKV) ([]interface{}, error) {
	res := make([]interface{}, len(kvs))
	for i, kv := range kvs {
		m, err := kvToMap(kv)
		if err != nil {
			return nil, err
		}
		res[i] = m
	}
	return res, nil
}

func kvToMap(kv *bt.VersionedKV) (map[string]interface{}, error) {
	j := toJSON(kv)
	var result map[string]interface{}
	if err := json.Unmarshal([]byte(j), &result); err != nil {
		return nil, fmt.Errorf("failed to unmarshal: %v", err)
	}
	return result, nil
}

func toJSON(v interface{}) string {
	out, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		panic(err)
	}
	return string(out)
}

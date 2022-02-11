//go:build js && wasm
// +build js,wasm

package wasm

import (
	"fmt"
	"syscall/js"

	"github.com/elh/bitempura"
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
		fmt.Println("ERROR: failed to init db!")
	}
	_ = db

	fmt.Println("INFO: db initialized.")
}

// Get is the wasm adapter for DB.Get
func Get(this js.Value, inputs []js.Value) interface{} {
	fmt.Println("unimplemented")
	return nil
}

// List is the wasm adapter for DB.List
func List(this js.Value, inputs []js.Value) interface{} {
	fmt.Println("unimplemented")
	return nil
}

// Set is the wasm adapter for DB.Set
func Set(this js.Value, inputs []js.Value) interface{} {
	fmt.Println("unimplemented")
	return nil
}

// Delete is the wasm adapter for DB.Delete
func Delete(this js.Value, inputs []js.Value) interface{} {
	fmt.Println("unimplemented")
	return nil
}

// History is the wasm adapter for DB.History
func History(this js.Value, inputs []js.Value) interface{} {
	fmt.Println("unimplemented")
	return nil
}

// SetNow is the wasm adapter for dbtest.TestClock.SetNow
func SetNow(this js.Value, inputs []js.Value) interface{} {
	fmt.Println("unimplemented")
	return nil
}

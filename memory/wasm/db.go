package wasm

import (
	"fmt"

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

	fmt.Println("INFO: db initialized!")
}

// TODO is just here to trigger the main import
// TODO: remove this
var TODO = "TODO: remove"

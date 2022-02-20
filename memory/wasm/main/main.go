//go:build js && wasm
// +build js,wasm

package main

import (
	"syscall/js"

	"github.com/elh/bitempura/memory/wasm"
)

// All functions are exported with the "bt_" prefix.
// The working model for execution in Wasm is that there is one global memory.DB.
func main() {
	c := make(chan struct{})
	// db functions
	js.Global().Set("bt_Get", js.FuncOf(wasm.Get))
	js.Global().Set("bt_List", js.FuncOf(wasm.List))
	js.Global().Set("bt_Set", js.FuncOf(wasm.Set))
	js.Global().Set("bt_Delete", js.FuncOf(wasm.Delete))
	js.Global().Set("bt_History", js.FuncOf(wasm.History))
	// helpers
	js.Global().Set("bt_SetNow", js.FuncOf(wasm.SetNow))
	js.Global().Set("bt_OnChange", js.FuncOf(wasm.OnChange))
	<-c
}

//go:build js && wasm
// +build js,wasm

package main

import (
	"syscall/js"

	"github.com/elh/bitempura/memory/wasm"
)

// All functions are exported with the "bt_" prefix.
// The working model for execution in Wasm is that there is one global memory.DB. bt_Init must be called before usage.
func main() {
	c := make(chan struct{})
	// init (and re-init)
	js.Global().Set("bt_Init", js.FuncOf(wasm.Init))
	// db functions
	js.Global().Set("bt_Get", js.FuncOf(wasm.Get))
	js.Global().Set("bt_List", js.FuncOf(wasm.List))
	js.Global().Set("bt_Set", js.FuncOf(wasm.Set))
	js.Global().Set("bt_Delete", js.FuncOf(wasm.Delete))
	js.Global().Set("bt_History", js.FuncOf(wasm.History))
	// helpers
	js.Global().Set("bt_OnChange", js.FuncOf(wasm.OnChange))
	js.Global().Set("bt_SetNow", js.FuncOf(wasm.SetNow))
	<-c
}

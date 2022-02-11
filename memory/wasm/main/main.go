//go:build js && wasm
// +build js,wasm

package main

import (
	"syscall/js"

	"github.com/elh/bitempura/memory/wasm"
)

// all functions are exports with the "bt_" prefix. the working model for the wasm file is that there is one global
// memory.DB local in the JavaScript environment
func main() {
	c := make(chan struct{})
	js.Global().Set("bt_Get", js.FuncOf(wasm.Get))
	js.Global().Set("bt_List", js.FuncOf(wasm.List))
	js.Global().Set("bt_Set", js.FuncOf(wasm.Set))
	js.Global().Set("bt_Delete", js.FuncOf(wasm.Delete))
	js.Global().Set("bt_History", js.FuncOf(wasm.History))
	js.Global().Set("bt_SetNow", js.FuncOf(wasm.SetNow))
	<-c
}

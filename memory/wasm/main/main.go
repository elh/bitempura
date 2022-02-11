package main

import "github.com/elh/bitempura/memory/wasm"

var _ = wasm.TODO // just trigger the init

func main() {
	c := make(chan struct{})
	// TODO: register functions
	<-c
}

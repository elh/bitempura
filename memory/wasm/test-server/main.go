// Package main starts up a local webserver for testing the wasm build
package main

import (
	"net/http"
)

const (
	appPort = "8080"
)

func main() {
	http.Handle("/", http.FileServer(http.Dir("./memory/wasm/test-server")))
	http.Handle("/assets/", http.StripPrefix("/assets/", http.FileServer(http.Dir("./memory/wasm/assets"))))

	if err := http.ListenAndServe(":"+appPort, nil); err != nil {
		panic(err)
	}
}

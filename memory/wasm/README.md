# memory/wasm

Provides a WebAssembly adapter for memory.DB.

`main/` contains the registrations for `db.go` to WebAssembly. This is built into a gitignored `asserts/` dir.

`make test-wasm` starts the `test-server/` server that serves the `.wasm` files for testing.

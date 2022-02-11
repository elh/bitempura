# memory/wasm 🧩

Provides compilation of `memory.DB` to WebAssembly.

`main/` contains the registrations for callable functions and is built into the git-ignored `asserts/` dir. The working model for execution is that there is one global memory.DB. All functions are exported with the `bt_` prefix.

### Testing

`make test-wasm` starts the `test-server/` server that makes the `.wasm` files available at `localhost:8080`. Try running `bt_List()` in the javascript console.

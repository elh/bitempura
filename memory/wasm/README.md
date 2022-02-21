# memory/wasm 🧩

Provides compilation of `memory.DB` to WebAssembly.

`main/` contains the registrations for callable functions and is built into the git-ignored `asserts/` dir. The working model for execution is that there is one global memory.DB. `bt_Init` must be called before usage. All functions are exported with the `bt_` prefix.

```
// Init initializes the global Wasm DB. bt_Init must be called before usage.
// arguments = [withClock: bool]

// Get is the wasm adapter for DB.Get.
// arguments = key: string, [as_of_valid_time: string (RFC 3339 datetime), as_of_transaction_time: string (RFC 3339 datetime)]

// List is the wasm adapter for DB.List.
// arguments = [as_of_valid_time: string (RFC 3339 datetime), as_of_transaction_time: string (RFC 3339 datetime)]

// Set is the wasm adapter for DB.Set.
// arguments = key: string, value: string (JSON string), [with_valid_time: string (RFC 3339 datetime), with_end_valid_time: string (RFC 3339 datetime)]

// Delete is the wasm adapter for DB.Delete.
// arguments = key: string, [with_valid_time: string (RFC 3339 datetime), with_end_valid_time: string (RFC 3339 datetime)]

// History is the wasm adapter for DB.History.
// arguments = key: string

// OnChange allows the user to register a callback function to be invoked when the database changes. The callback
// function is invoked with the key that was just updated.
// arguments = fn: unary function (arguments = key: string)

// SetNow is the wasm adapter for dbtest.TestClock.SetNow. SetNow can only be called if DB was bt.Init-ed with a clock.
// arguments = now: string (RFC 3339 datetime)
```

### Testing

`make test-wasm` starts the `test-server/` server that makes the `.wasm` files available at `localhost:8080`. Try running `bt_List()` in the javascript console.

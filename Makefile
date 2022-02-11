.PHONY: error cp-wasm-exec build-wasm test-wasm

error:
	@echo "specify make target"
	@exit 1

# copy Go's wasm-exec to this project
cp-wasm-exec:
	mkdir -p memory/wasm/assets
	cp $$(go env GOROOT)/misc/wasm/wasm_exec.js memory/wasm/assets/.

# build memory/wasm and place in assets dir
build-wasm:
	mkdir -p memory/wasm/assets
	(cd memory/wasm/main && GOOS=js GOARCH=wasm go build -o main.wasm && mv main.wasm ../assets/.)

# starts up a local webserver for testing the wasm build
test-wasm: build-wasm cp-wasm-exec
	go run memory/wasm/test-server/main.go

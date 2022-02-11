.PHONY: error build test lint cp-wasm-exec build-wasm test-wasm

error:
	@echo "specify make target"
	@exit 1

# build
# build wasm packages separately
build:
	go build `go list ./... | grep -v wasm`
	GOOS=js GOARCH=wasm go build `go list ./... | grep wasm`

# test w/ output history
test:
	go test ./... -output-history

# test w/ output history and fail if any output changes are found
test-check-output:
	go test ./... -output-history
	git diff --exit-code

# lint
# run golint because I like the exported comment warnings. It does not fail the run.
lint:
	golint ./...
	golangci-lint run

######################################################### wasm #########################################################

# copy Go's wasm_exec to this project
cp-wasm-exec:
	mkdir -p memory/wasm/assets
	cp $$(go env GOROOT)/misc/wasm/wasm_exec.js memory/wasm/assets/.

# build memory/wasm and place in assets dir
build-wasm:
	mkdir -p memory/wasm/assets
	(cd memory/wasm/main && GOOS=js GOARCH=wasm go build -o main.wasm && mv main.wasm ../assets/.)

# starts up a local webserver for testing the wasm build
test-wasm: cp-wasm-exec build-wasm
	go run memory/wasm/test-server/main.go

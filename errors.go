package bitempura

import "errors"

// ErrNotFound error is returned when key not found in DB (as of relevant valid and transaction times).
var ErrNotFound = errors.New("not found")

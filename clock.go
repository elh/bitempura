package bitempura

import "time"

// Clock is an interface for providing the current time for database to use as transaction times.
type Clock interface {
	Now() time.Time
}

// DefaultClock is a default clock that implements Now() with time.Now()
type DefaultClock struct{}

// Now returns time.Now()
func (c *DefaultClock) Now() time.Time {
	return time.Now()
}

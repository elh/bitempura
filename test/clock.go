package test

import (
	"fmt"
	"time"
)

// TestClock is a clock returns user-set times for testing. It enforces that new times being set must be monotonically
// increasing as a safeguard for correct tests.
type TestClock struct {
	now time.Time
}

// Now returns user-set time for testing
func (c *TestClock) Now() time.Time {
	return c.now
}

// SetNow sets "now" returned by the DB for transaction times. Times being set must be monotonically increasing.
func (c *TestClock) SetNow(t time.Time) error {
	if c.now.After(t) {
		return fmt.Errorf("TestClock: times must be monotonically increasing")
	}
	c.now = t
	return nil
}

package test

import (
	"fmt"
	"sync"
	"time"
)

// TestClock is a clock returns user-set times for testing. It enforces that new times being set must be monotonically
// increasing as a safeguard for correct tests.
type TestClock struct {
	now time.Time
	m   sync.RWMutex
}

// Now returns user-set time for testing
func (c *TestClock) Now() time.Time {
	c.m.RLock()
	defer c.m.RUnlock()
	return c.now
}

// SetNow sets "now" returned by the DB for transaction times. Times being set must be monotonically increasing.
func (c *TestClock) SetNow(t time.Time) error {
	c.m.Lock()
	defer c.m.Unlock()
	if c.now.After(t) {
		return fmt.Errorf("TestClock: times must be monotonically increasing")
	}
	c.now = t
	return nil
}

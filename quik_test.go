package quik

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TaskChecker is a helper for tests that can ensure tasks did not get
// dropped and ran to completion etc.
//
// More utility to come in future here.
type TaskChecker struct {
	hits int
}

// TODO: Build out this suite later, we are in very early stages right now.
func TestQuikPoolCallsTasks(t *testing.T) {
	c := TaskChecker{}
	p := New(WithWorkerLimit[int](1))
	p.EnqueueWait(func() int {
		fmt.Println("running task")
		c.hits++
		return 10
	})
	p.Stop(true)
	assert.Equal(t, c.hits, 1)
}

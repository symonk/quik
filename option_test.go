package quik

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAppliesMaxWorkersCorrectly(t *testing.T) {
	w := 10
	p := New(WithWorkerLimit[any](w))
	assert.Equal(t, p.maxWorkers, w)
}

func TestMaxWorkersDefaultIsOne(t *testing.T) {
	p := New[any]()
	assert.Equal(t, p.maxWorkers, 1)
}

func TestCanOverwriteWorkerIdleDuration(t *testing.T) {
	s := time.Second
	p := New(WithWorkerIdleCheckDuration[any](s))
	assert.Equal(t, p.workerScaleInterval, s)
}

func TestDefaultWorkerIdleDuration(t *testing.T) {
	p := New[any]()
	assert.Equal(t, p.workerScaleInterval, defaultWorkerCyclePeriod)
}

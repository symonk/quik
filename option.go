package quik

import "time"

// Option sets a parameter for the pool.
type Option[T any] func(p *Pool[T])

// Limit the upper bounded number of goroutines allowed to run
func WithWorkerLimit[T any](maxworkers int) Option[T] {
	return func(p *Pool[T]) {
		p.maxWorkers = maxworkers
	}
}

// WithWorkerIdleCheckDuration configures how frequently the pool
// will attempt to check for idle workers and shut one of them
// down when under smaller load.
func WithWorkerIdleCheckDuration[T any](d time.Duration) Option[T] {
	return func(p *Pool[T]) {
		p.workerScaleInterval = d
	}
}

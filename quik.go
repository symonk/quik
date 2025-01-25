package quik

import (
	"context"
	"fmt"
	"sync"
	"time"
)

/*
My concept for the worker pool will be:

A priority based task enqueue system
Run indefinitely, or load in tasks and wait until completion API
Event based system to report events for scaling actions to the user (if desired)
Generic implementation that can yield results back to the caller
Throttleable, immediately halt tasks - i.e a database is maybe failing over etc.
Fast!
Auto scaling workers
benchmarked, profiled etc
No blocking, will always accept tasks - careful on memory limits - functional option for inspecting - Add guard rails!

Incoming tasks go through this process:
	-> Incoming tasks are always accepted
		-> If there is room, move them directly into the workers queue
		-> if there is no room, store them in the 'waiting pen'

*/

const (
	defaultWorkerCyclePeriod = 3 * time.Second
)

// Pooler defines the public interface of the pool
type Pooler[T any] interface {
	Stop(graceful bool)

	// TODO: Group these into a single Enqueue method that takes its own options.
	Enqueue(task func() T)
	EnqueuePriority(task func() T, priority int)
	EnqueueWait(task func() T)
	EnqueuePriorityWait(task func() T)

	Quiesce(ctx context.Context)
	Drain()
}

// Pool is the core worker pool implementation for quik.
// It is highly configurable via a number of different options.
// Placeholder: Options outline.
type Pool[T any] struct {
	// worker specifics
	maxworkers          int
	workerScaleInterval time.Duration

	results chan T
	done    chan struct{}
	closing chan struct{}

	stopped   bool
	stoppedMu sync.Mutex

	// Incoming Task Queue Specifics
	inboundTasks chan func() T

	// Waiting Pen Specifics
	waitingQ chan func() T

	// Worker Queue Specifics
	workerQ chan func() T
}

// New instantiates a new pool instance, applying the options
// configuration to it and returning the pointer to the instance.
func New[T any](options ...Option[T]) *Pool[T] {
	pool := &Pool[T]{
		maxworkers:          1,
		workerScaleInterval: defaultWorkerCyclePeriod,
		results:             make(chan T),
		done:                make(chan struct{}),
		inboundTasks:        make(chan func() T),
		closing:             make(chan struct{}),
		// TODO: Change underling data structure here, this can still block
		// when producer is faster than consumers.
		waitingQ: make(chan func() T, 1024),
		workerQ:  make(chan func() T),
	}

	for _, optFn := range options {
		optFn(pool)
	}
	go pool.run()
	return pool
}

func (p *Pool[T]) run() {
	defer close(p.done)

	var currentWorkers int
	// Keep track if we have been idle for the worker idle period of time
	// if we have, attempt to shutdown a worker
	workerCheckTicker := time.NewTicker(defaultWorkerCyclePeriod)
	idle := false
core:
	for {

		// There is backpressure at the moment in the form of a backlog
		// try get the tasks out of there into the worker queue directly.
		if len(p.waitingQ) != 0 {
			moved := p.processWaitingQueue()
			if moved {
				continue
			}
		}

		// There are no tasks in the holding pen, we can try to transfer tasks
		// directly from the incoming task to the worker queue.
		// If the worker queue is blocking, move them into the holding pen.
		select {
		// We can transfer from the incoming queue directly into workers.
		case t := <-p.inboundTasks:
			idle = false

			// Handle worker creation if required, otherwise we will be blocked
			// indefinitely on the first task as the worker queue will never
			// be accepting a task
			if currentWorkers < p.maxworkers {
				// we need a synchronisation event here to avoid the worker routine
				// never getting any scheduler time.
				p.startWorker()
				currentWorkers++
			}

			select {
			// TODO: This will do the write before we've launched a worker
			case p.workerQ <- t:
			default:
				// TODO: This worker routine is completely starved right now - probably
				// by the busy loop on select default!
				// Remove the sleep later and fix.
				time.Sleep(time.Second)
				// This is blocking for now, but we don't want it to be;
				// swap out the channel FIFO queue for something more
				// performant at both ends (insert[0], popright)
				p.waitingQ <- t
			}
		case <-workerCheckTicker.C:
			if idle {
				p.shutdownWorker()
			}
			workerCheckTicker.Reset(defaultWorkerCyclePeriod)
		case <-p.closing:
			break core
		}
	}
}

func (p *Pool[T]) terminate(graceful bool) {
	close(p.closing)
	<-p.done // wait until run() has fully terminated.
}

func (p *Pool[T]) Stop(graceful bool) {
	p.terminate(graceful)
}

// IsStopped returns a boolean indicating if the pool
// has been successfully stopped.
func (p *Pool[T]) IsStopped() bool {
	p.stoppedMu.Lock()
	defer p.stoppedMu.Unlock()
	return p.stopped
}

func (p *Pool[T]) Enqueue(task func() T) {
	if task != nil {
		p.inboundTasks <- task
	}
}
func (p *Pool[T]) EnqueuePriority(task func() T) {
	if task != nil {
		p.inboundTasks <- task
	}
}

// EnqueueWait submits a task into the pool and blocks until
// the task has been processed.
// This internally wraps the task function in an internal function
// and waits until it has been exited.
func (p *Pool[T]) EnqueueWait(task func() T) {
	closeCh := make(chan struct{})
	wrapper := func() T {
		defer func() {
			fmt.Println("closing task chan")
			close(closeCh)
		}()
		result := task()
		fmt.Println("wrapped result: ", result)
		return result
	}
	p.inboundTasks <- wrapper
	<-closeCh
}

// processWaitingQueue attempts to pick tasks from the holding pen and
// moves them towards the worker queue.  If we have a backlog of
// pending tasks we should prioritise taking from here instead.
//
// TODO: Figure out priority fits into this sytem, it doesn't as
// a high priority task should be taken regardless of this 'backlog'
// This cannot be blocking however.
//
// This will avoid spinning the CPU, it will attempt to move a single
// task, the core run loop will continue to try as long as the length
// of the holding pen is not empty.
func (p *Pool[T]) processWaitingQueue() bool {
	select {
	case p.workerQ <- <-p.waitingQ:
		return true
	default:
		return false
	}
}

// shutdownWorker sends a nil task to a worker which would cause them to exit.
// this is called when the pool has been idle for the configured period of
// time.
//
// The overhead in stopping these workers is minimal, this maximises efficiency.
func (p *Pool[T]) shutdownWorker() {
	p.workerQ <- nil
}

// startWorker spawns a new groutine with another worker.
// this is only invoked if the current workers is less than
// or equal to the configured pool maxworkers
//
// currworkers is evaluated in the core loop and not here
// to avoid the need for excessive locking.
func (p *Pool[T]) startWorker() {
	go worker(p.workerQ)
}

// worker is ran in a goroutine upto pool.maxworker times.
// if a nil task is received by the worker, it is considered
// a signal to shutdown.
func worker[T any](workerTaskQueue <-chan func() T) {
	fmt.Println("started worker!")
	for t := range workerTaskQueue {
		fmt.Println("got task!")
		if t == nil {
			fmt.Println("shutting down worker")
			return
		}
		r := t()
		fmt.Println("result was: ", r)
	}
}

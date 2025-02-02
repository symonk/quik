package quik

import (
	"context"
	"sync"
	"sync/atomic"
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
	maxWorkers          int
	currentWorkers      int
	workerScaleInterval time.Duration

	results chan T
	done    chan struct{}

	stopped    bool
	stoppedMu  sync.Mutex
	once       *sync.Once
	stopSignal chan bool

	// Incoming Task Queue Specifics
	inboundTasks chan func() T

	// Waiting Pen Specifics
	waitingQ       chan func() T
	waitingQLength int32

	// Worker Queue Specifics
	workerQ chan func() T
}

// New instantiates a new pool instance, applying the options
// configuration to it and returning the pointer to the instance.
func New[T any](options ...Option[T]) *Pool[T] {
	pool := &Pool[T]{
		maxWorkers:          1,
		workerScaleInterval: defaultWorkerCyclePeriod,
		results:             make(chan T),
		done:                make(chan struct{}),
		inboundTasks:        make(chan func() T),
		// TODO: Change underling data structure here, this can still block
		// when producer is faster than consumers.
		waitingQ:   make(chan func() T, 1024),
		workerQ:    make(chan func() T),
		stopSignal: make(chan bool, 1),
	}

	for _, optFn := range options {
		optFn(pool)
	}
	go pool.run()
	return pool
}

func (p *Pool[T]) run() {
	defer close(p.done)

	// Keep track if we have been idle for the worker idle period of time
	// if we have, attempt to shutdown a worker
	workerCheckTicker := time.NewTicker(defaultWorkerCyclePeriod)
	idle := false

	var workerWg sync.WaitGroup
core:
	for {
		// There is backpressure at the moment in the form of a backlog
		// try get the tasks out of there into the worker queue directly.
		if p.WaitingQueueSize() != 0 {
			ok := p.processWaitingTask()
			if ok {
				continue
			}
		}

		// There are no tasks in the holding pen, we can try to transfer tasks
		// directly from the incoming task to the worker queue.
		// If the worker queue is blocking, move them into the holding pen.
		select {
		// We can transfer from the incoming queue directly into workers.
		case t, ok := <-p.inboundTasks:
			if !ok {
				// Stop() has been invoked.
				break core
			}
			select {
			// Try to push the task directly into the worker queue.
			case p.workerQ <- t:
			// WorkerQ woud be blocking, store the task in the waiting queue.
			default:
				// Handle worker creation if required, otherwise we will be blocked
				// indefinitely on the first task as the worker queue will never
				// be accepting a task
				if p.currentWorkers < p.maxWorkers {
					// we need a synchronisation event here to avoid the worker routine
					// never getting any scheduler time.
					p.startWorker(t, &workerWg)
				} else {
					p.waitingQ <- t
				}
				atomic.StoreInt32(&p.waitingQLength, int32(len(p.waitingQ)))
			}
		case <-workerCheckTicker.C:
			if idle && p.currentWorkers != 0 {
				p.shutdownWorker()
			}
			workerCheckTicker.Reset(defaultWorkerCyclePeriod)
		}
	}

	graceful := <-p.stopSignal
	if graceful {
		p.Drain()
	}

	// While we have current workers, send nil tasks onto the worker queue
	// in order to force all the workers to finally exit, zero'ing the wg
	// for a clean exit.
	// TODO: What if we have tasks to drain down?
	for p.currentWorkers > 0 {
		p.shutdownWorker()
	}

	// Wait for all the workers to shutdown.
	workerWg.Wait()
}

// terminate signals run() to terminate (optionally)
// gracefully.  If graceful is true all internal queues
// are flushed and the call will be blocking until
// the pool has finalized cleanly.
//
// If graceful is false, the pool will exit faster but
// tasks in transient/flight will be lost and have no
// guarantee of completion.
func (p *Pool[T]) terminate(graceful bool) {
	p.once.Do(func() {
		p.stoppedMu.Lock()
		p.stopped = true
		p.stoppedMu.Unlock()
		p.stopSignal <- graceful
		close(p.inboundTasks)
	})
	<-p.done
}

// Stop terminates the pool.
func (p *Pool[T]) Stop(graceful bool) {
	p.terminate(graceful)
}

// Drain is invoked when Stop() is called if a graceful termination is
// requested.  This blocks the incoming queues and is blocking until
// all tasks currently in the queue systems internally are cleared down.
func (p *Pool[T]) Drain() {

}

// Quiesce causes the pool to pause internally until a particular context
// has been timed out.  This is useful for reacting to cases such as
// database issues where writes would be failing.
//
// All workers will be blocked.  For now it queues tasks to cause a block
// but in future this will utilise a priority system to guarantee instant
// blockage across all workers.
func (p *Pool[T]) Quiesce(context context.Context) {

}

// WaitingQueueSize returns the size of the holding pen.
func (p *Pool[T]) WaitingQueueSize() int32 {
	return atomic.LoadInt32(&p.waitingQLength)
}

// Stopped returns a boolean indicating if the pool
// has been successfully stopped.
func (p *Pool[T]) Stopped() bool {
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
		defer close(closeCh)
		result := task()
		return result
	}
	p.inboundTasks <- wrapper
	<-closeCh
}

// processWaitingTask attempts to pick tasks from the holding pen and
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
func (p *Pool[T]) processWaitingTask() bool {
	select {
	case t, ok := <-p.inboundTasks:
		if !ok {
			return false
		}
		p.waitingQ <- t
	case t := <-p.waitingQ:
		atomic.StoreInt32(&p.waitingQLength, p.waitingQLength-1)
		p.workerQ <- t
		return true
	}
	return true
}

// shutdownWorker sends a nil task to a worker which would cause them to exit.
// this is called when the pool has been idle for the configured period of
// time.
//
// The overhead in stopping these workers is minimal, this maximises efficiency.
func (p *Pool[T]) shutdownWorker() {
	p.workerQ <- nil
	p.currentWorkers--
}

// startWorker spawns a new groutine with another worker.
// this is only invoked if the current workers is less than
// or equal to the configured pool maxworkers
//
// currworkers is evaluated in the core loop and not here
// to avoid the need for excessive locking.
func (p *Pool[T]) startWorker(initialTask func() T, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		worker(initialTask, p.workerQ, wg)
	}()
	p.currentWorkers++
}

// worker is ran in a goroutine upto pool.maxworker times.
// if a nil task is received by the worker, it is considered
// a signal to shutdown.
func worker[T any](t func() T, workerTaskQueue <-chan func() T, wg *sync.WaitGroup) {
	defer wg.Done()
	for t != nil {
		t()
		t = <-workerTaskQueue
	}
}

package workgroup

import (
	"context"

	"github.com/tschaub/limited"
)

// WorkFunc is called by the worker with task data.  The work function receives a pointer to the
// worker so that it can add additional task data.
type WorkFunc[T any] func(*Worker[T], T) error

// Worker executes a group of tasks that may grow over time.
type Worker[T any] struct {
	ctx      context.Context
	limit    int
	queue    Queue[T]
	work     WorkFunc[T]
	groupCtx context.Context
}

// Options for constructing a new worker.  Only the Work function is required.  No more than the Limit
// number of tasks will be executed concurrently.
//
// By default, a limit of 1 is used and an in-memory queue is used.  You can provide your own queue
// that implements the Queue interface.
type Options[T any] struct {
	Context context.Context
	Limit   int
	Queue   Queue[T]
	Work    WorkFunc[T]
}

// New creates a new worker.
func New[T any](opts *Options[T]) *Worker[T] {
	worker := &Worker[T]{
		ctx:   opts.Context,
		limit: opts.Limit,
		queue: opts.Queue,
		work:  opts.Work,
	}

	if worker.ctx == nil {
		worker.ctx = context.Background()
	}
	if worker.limit <= 0 {
		worker.limit = 1
	}
	if worker.queue == nil {
		worker.queue = NewDefaultQueue[T]()
	}
	return worker
}

// Context returns the group context while tasks are being executed.
func (w *Worker[T]) Context() context.Context {
	return w.groupCtx
}

// Add queues up additional task data.
func (w *Worker[T]) Add(data T) error {
	return w.queue.Add(w.ctx, data)
}

// Wait blocks until all tasks have been executed.
func (w *Worker[T]) Wait() error {
	for w.queue.HasNext(w.ctx) {
		if err := w.waitOnBatch(); err != nil {
			return err
		}
	}
	return nil
}

func (w *Worker[T]) waitOnBatch() error {
	group, groupCtx := limited.WithContext(w.ctx, w.limit+1)
	w.groupCtx = groupCtx
	err := group.Go(func() error {
		for {
			data, nextErr := w.queue.Next(w.ctx)
			if nextErr != nil {
				if nextErr == ErrEmptyQueue {
					return nil
				}
				return nextErr
			}

			err := group.Go(func() error {
				return w.work(w, data)
			})
			if err != nil {
				return err
			}
		}
	})
	if err != nil {
		return err
	}
	return group.Wait()
}

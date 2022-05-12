package workgroup

import (
	"context"
	"errors"
	"sync"
)

// ErrEmptyEqueue is returned when the queue is empty.
var ErrEmptyQueue = errors.New("empty queue")

// Queue holds task data to be executed.
//
// By default, the workgroup uses an in-memory queue.  You can provide your own queue
// that implements this interface.
type Queue[T any] interface {
	// Add task data to the queue.
	Add(ctx context.Context, data T) error

	// Next task data in the queue.  Returns ErrEmptyQueue if the queue is empty.
	Next(ctx context.Context) (data T, err error)

	// HasNext returns true if the queue has more data.
	HasNext(ctx context.Context) bool
}

type sliceQueue[T any] struct {
	buffer []T
	mutex  *sync.Mutex
}

func newSliceQueue[T any]() *sliceQueue[T] {
	return &sliceQueue[T]{
		mutex: &sync.Mutex{},
	}
}

func (q *sliceQueue[T]) Add(ctx context.Context, data T) error {
	q.mutex.Lock()
	q.buffer = append(q.buffer, data)
	q.mutex.Unlock()
	return nil
}

func (q *sliceQueue[T]) Next(ctx context.Context) (T, error) {
	q.mutex.Lock()
	if len(q.buffer) == 0 {
		q.mutex.Unlock()
		return *new(T), ErrEmptyQueue
	}
	data := q.buffer[0]
	q.buffer = q.buffer[1:]
	q.mutex.Unlock()
	return data, nil
}

func (q *sliceQueue[T]) HasNext(ctx context.Context) bool {
	return len(q.buffer) > 0
}

var _ Queue[interface{}] = (*sliceQueue[interface{}])(nil)

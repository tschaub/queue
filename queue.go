package queue

import (
	"context"
)

type Queue[T any] struct {
	buffer         []T
	ctx            context.Context
	addLock        chan struct{}
	removeLock     chan struct{}
	nonEmptySignal chan struct{}
}

func (q *Queue[T]) acquireAddLock() error {
	select {
	case <-q.ctx.Done():
		return q.ctx.Err()
	case q.addLock <- struct{}{}:
		return nil
	}
}

func (q *Queue[T]) releaseAddLock() {
	<-q.addLock
}

func (q *Queue[T]) acquireRemoveLock() error {
	select {
	case <-q.ctx.Done():
		return q.ctx.Err()
	case q.removeLock <- struct{}{}:
		return nil
	}
}

func (q *Queue[T]) releaseRemoveLock() {
	<-q.removeLock
}

func WithContext[T any](ctx context.Context) *Queue[T] {
	q := &Queue[T]{
		ctx:            ctx,
		buffer:         []T{},
		addLock:        make(chan struct{}, 1),
		removeLock:     make(chan struct{}, 1),
		nonEmptySignal: make(chan struct{}, 1),
	}
	return q
}

func (q *Queue[T]) Add(item T) error {
	if err := q.acquireAddLock(); err != nil {
		return err
	}
	defer q.releaseAddLock()
	q.buffer = append(q.buffer, item)
	if len(q.buffer) == 1 {
		q.nonEmptySignal <- struct{}{}
	}
	return nil
}

func (q *Queue[T]) Remove() (T, error) {
	zero := *new(T)
	if err := q.acquireRemoveLock(); err != nil {
		return zero, err
	}
	defer q.releaseRemoveLock()

	if len(q.buffer) == 0 {
		select {
		case <-q.ctx.Done():
			return zero, q.ctx.Err()
		case <-q.nonEmptySignal:
			// pass
		}
	}

	if err := q.acquireAddLock(); err != nil {
		return zero, err
	}
	defer q.releaseAddLock()

	item := q.buffer[0]
	q.buffer[0] = zero
	q.buffer = q.buffer[1:]

	if len(q.buffer) == 0 && len(q.nonEmptySignal) == 1 {
		<-q.nonEmptySignal
	}

	return item, nil
}

func (q *Queue[T]) Close() {
	close(q.addLock)
	close(q.removeLock)
	close(q.nonEmptySignal)
	q.buffer = nil
}

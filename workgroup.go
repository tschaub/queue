package workgroup

import (
	"context"
	"sync"

	"github.com/tschaub/limited"
)

type WorkFunc[T any] func(context.Context, *Worker[T], T) error

type Worker[T any] struct {
	Context context.Context
	Limit   int
	Work    WorkFunc[T]
	buffer  []T
	mutex   *sync.Mutex
}

func (w *Worker[T]) Add(data T) {
	if w.mutex == nil {
		w.mutex = &sync.Mutex{}
	}

	w.mutex.Lock()
	w.buffer = append(w.buffer, data)
	w.mutex.Unlock()
}

func (w *Worker[T]) Wait() error {
	for len(w.buffer) > 0 {
		if err := w.waitOnBatch(); err != nil {
			return err
		}
	}
	return nil
}

func (w *Worker[T]) waitOnBatch() error {
	ctx := w.Context
	if ctx == nil {
		ctx = context.Background()
	}
	group, groupCtx := limited.WithContext(ctx, int64(w.Limit+1))
	err := group.Go(func() error {
		for len(w.buffer) > 0 {
			w.mutex.Lock()
			item := w.buffer[0]
			w.buffer = w.buffer[1:]
			w.mutex.Unlock()

			err := group.Go(func() error {
				return w.Work(groupCtx, w, item)
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return group.Wait()
}

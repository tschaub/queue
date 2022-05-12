package workgroup_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tschaub/workgroup"
)

func ExampleWorker() {
	worker := workgroup.New(workgroup.Options[string]{
		Work: func(w *workgroup.Worker[string], data string) error {
			if len(data) == 0 {
				return nil
			}

			// do some work
			fmt.Printf("working on %s...\n", data)
			time.Sleep(10 * time.Millisecond)

			// spawn more work
			err := w.Add(data[1:])
			if err != nil {
				fmt.Printf("unexpected errror: %s\n", err)
			}

			return nil
		},
	})

	err := worker.Add("abcdef")
	if err != nil {
		fmt.Printf("unexpected errror: %s\n", err)
	}

	err = worker.Wait()
	if err != nil {
		fmt.Printf("unexpected errror: %s\n", err)
	}

	// Output:
	// working on abcdef...
	// working on bcdef...
	// working on cdef...
	// working on def...
	// working on ef...
	// working on f...
}

func ExampleWorker_context() {
	ctx, cancel := context.WithCancel(context.Background())

	worker := workgroup.New(workgroup.Options[string]{
		Context: ctx,
		Work: func(w *workgroup.Worker[string], data string) error {
			if len(data) == 3 {
				cancel()
				return nil
			}

			// do some work
			fmt.Printf("working on %s...\n", data)
			time.Sleep(10 * time.Millisecond)

			// spawn more work
			err := w.Add(data[1:])
			if err != nil {
				fmt.Printf("unexpected errror: %s\n", err)
			}

			return nil
		},
	})

	err := worker.Add("abcdef")
	if err != nil {
		fmt.Printf("unexpected errror: %s\n", err)
	}

	err = worker.Wait()
	if err != nil {
		fmt.Printf("unexpected errror: %s\n", err)
	}

	// Output:
	// working on abcdef...
	// working on bcdef...
	// working on cdef...
}

func TestWorker(t *testing.T) {
	visited := sync.Map{}
	letters := "abcdefghijklmnopqrstuvwxyz"

	worker := workgroup.New(workgroup.Options[string]{
		Work: func(w *workgroup.Worker[string], data string) error {
			assert.NotNil(t, w.Context())
			visited.Store(data, true)
			return nil
		},
	})

	for i := 0; i < len(letters); i++ {
		err := worker.Add(letters[i : i+1])
		require.NoError(t, err)
	}

	err := worker.Wait()
	assert.NoError(t, err)

	for i := 0; i < len(letters); i++ {
		_, ok := visited.Load(letters[i : i+1])
		assert.True(t, ok)
	}
}

func TestWorkerError(t *testing.T) {
	letters := "abcdefghijklmnopqrstuvwxyz"

	expectedErr := errors.New("expected")

	worker := workgroup.New(workgroup.Options[string]{
		Limit: 10,
		Work: func(w *workgroup.Worker[string], data string) error {
			if data == "f" {
				return expectedErr
			}
			return nil
		},
	})

	for i := 0; i < len(letters); i++ {
		require.NoError(t, worker.Add(letters[i:i+1]))
	}

	err := worker.Wait()
	assert.Equal(t, expectedErr, err)
}

func TestWorkerContextCancelBeforeWait(t *testing.T) {
	visited := sync.Map{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	letters := "abcdefghijklmnopqrstuvwxyz"

	worker := workgroup.New(workgroup.Options[string]{
		Context: ctx,
		Limit:   10,
		Work: func(w *workgroup.Worker[string], data string) error {
			return nil
		},
	})

	for i := 0; i < len(letters); i++ {
		letter := letters[i : i+1]
		require.NoError(t, worker.Add(letter))
		if letter == "f" {
			cancel()
		}
	}

	err := worker.Wait()
	assert.Equal(t, context.Canceled, err)

	for i := 0; i < len(letters); i++ {
		letter := letters[i : i+1]
		t.Run(letter, func(t *testing.T) {
			_, ok := visited.Load(letter)
			assert.False(t, ok)
		})
	}
}

func TestWorkerLimit(t *testing.T) {
	visited := sync.Map{}
	letters := "abcdefghijklmnopqrstuvwxyz"

	worker := workgroup.New(workgroup.Options[string]{
		Limit: 5,
		Work: func(w *workgroup.Worker[string], data string) error {
			visited.Store(data, true)
			return nil
		},
	})

	for i := 0; i < len(letters); i++ {
		require.NoError(t, worker.Add(letters[i:i+1]))
	}

	err := worker.Wait()
	assert.NoError(t, err)

	for i := 0; i < len(letters); i++ {
		letter := letters[i : i+1]
		t.Run(letter, func(t *testing.T) {
			_, ok := visited.Load(letter)
			assert.True(t, ok)
		})
	}
}

func TestWorkerRecursive(t *testing.T) {
	visited := sync.Map{}
	letters := "abcdefghijklmnopqrstuvwxyz"

	worker := workgroup.New(workgroup.Options[string]{
		Limit: 1,
		Work: func(w *workgroup.Worker[string], data string) error {
			if len(data) == 1 {
				visited.Store(data, true)
				return nil
			}

			half := len(data) / 2
			require.NoError(t, w.Add(data[:half]))
			require.NoError(t, w.Add(data[half:]))
			return nil
		},
	})

	require.NoError(t, worker.Add(letters))

	err := worker.Wait()
	assert.NoError(t, err)

	for i := 0; i < len(letters); i++ {
		_, ok := visited.Load(letters[i : i+1])
		assert.True(t, ok)
	}
}

func TestWorkerRecursiveLimit(t *testing.T) {
	visited := sync.Map{}
	letters := "abcdefghijklmnopqrstuvwxyz"

	worker := workgroup.New(workgroup.Options[string]{
		Limit: 4,
		Work: func(w *workgroup.Worker[string], data string) error {
			if len(data) == 1 {
				visited.Store(data, true)
				return nil
			}

			for i := 0; i < len(data); i++ {
				require.NoError(t, w.Add(data[i:i+1]))
			}
			return nil
		},
	})

	require.NoError(t, worker.Add(letters))

	err := worker.Wait()
	assert.NoError(t, err)

	for i := 0; i < len(letters); i++ {
		_, ok := visited.Load(letters[i : i+1])
		assert.True(t, ok)
	}
}

package queue_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tschaub/queue"
	"golang.org/x/sync/errgroup"
)

func ExampleQueue() {
	q := queue.WithContext[string](context.Background())
	defer q.Close()

	err := q.Add("first")
	if err != nil {
		panic(err)
	}
	err = q.Add("second")
	if err != nil {
		panic(err)
	}

	first, err := q.Remove()
	if err != nil {
		panic(err)
	}
	second, err := q.Remove()
	if err != nil {
		panic(err)
	}

	fmt.Println(first)
	fmt.Println(second)
	// Output:
	// first
	// second
}

func ExampleWithContext() {
	ctx, cancel := context.WithCancel(context.Background())

	q := queue.WithContext[int](ctx)
	defer q.Close()

	err := q.Add(42)
	if err != nil {
		panic(err)
	}

	value, err := q.Remove()
	if err != nil {
		panic(err)
	}
	fmt.Println(value)

	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	// this will block until the cancel call above
	_, err = q.Remove()

	fmt.Println(err)
	// Output:
	// 42
	// context canceled
}

func TestQueue(t *testing.T) {
	q := queue.WithContext[string](context.Background())
	defer q.Close()

	firstAddErr := q.Add("first")
	assert.NoError(t, firstAddErr)

	secondAddErr := q.Add("second")
	assert.NoError(t, secondAddErr)

	thirdAddErr := q.Add("third")
	assert.NoError(t, thirdAddErr)

	first, firstErr := q.Remove()
	assert.NoError(t, firstErr)
	assert.Equal(t, "first", first)

	fourthAddErr := q.Add("fourth")
	assert.NoError(t, fourthAddErr)

	second, secondErr := q.Remove()
	assert.NoError(t, secondErr)
	assert.Equal(t, "second", second)

	third, thirdErr := q.Remove()
	assert.NoError(t, thirdErr)
	assert.Equal(t, "third", third)

	fourth, fourthErr := q.Remove()
	assert.NoError(t, fourthErr)
	assert.Equal(t, "fourth", fourth)
}

func TestRemoveBlocksWhenEmpty(t *testing.T) {
	q := queue.WithContext[string](context.Background())
	defer q.Close()

	firstAddErr := q.Add("first")
	assert.NoError(t, firstAddErr)

	secondAddErr := q.Add("second")
	assert.NoError(t, secondAddErr)

	first, firstErr := q.Remove()
	assert.NoError(t, firstErr)
	assert.Equal(t, "first", first)

	second, secondErr := q.Remove()
	assert.NoError(t, secondErr)
	assert.Equal(t, "second", second)

	go func() {
		time.Sleep(10 * time.Millisecond)
		thirdAddErr := q.Add("third")
		assert.NoError(t, thirdAddErr)
	}()

	third, thirdErr := q.Remove()
	assert.NoError(t, thirdErr)
	assert.Equal(t, "third", third)
}

func TestRemoveBlocksWhenEmptyUntilCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	q := queue.WithContext[string](ctx)
	defer q.Close()

	firstAddErr := q.Add("first")
	assert.NoError(t, firstAddErr)

	secondAddErr := q.Add("second")
	assert.NoError(t, secondAddErr)

	first, firstErr := q.Remove()
	assert.NoError(t, firstErr)
	assert.Equal(t, "first", first)

	second, secondErr := q.Remove()
	assert.NoError(t, secondErr)
	assert.Equal(t, "second", second)

	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	third, thirdErr := q.Remove()
	assert.Equal(t, context.Canceled, thirdErr)
	assert.Equal(t, "", third)
}

func TestAddConcurrent(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	q := queue.WithContext[int](context.Background())
	defer q.Close()

	count := 10
	for i := 0; i < count; i++ {
		go func(v int) {
			time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
			err := q.Add(v)
			assert.NoError(t, err)
		}(i)
	}

	removed := map[int]bool{}
	for i := 0; i < count; i++ {
		v, err := q.Remove()
		assert.NoError(t, err)
		assert.False(t, removed[v])
		removed[v] = true
	}
}

func TestAddAndRemoveConcurrent(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	group, ctx := errgroup.WithContext(context.Background())

	q := queue.WithContext[int](ctx)
	defer q.Close()

	count := 10
	for i := 0; i < count; i++ {
		v := i
		group.Go(func() error {
			time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
			return q.Add(v)
		})
	}

	for i := 0; i < count; i++ {
		group.Go(func() error {
			time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
			_, err := q.Remove()
			return err
		})
	}

	assert.NoError(t, group.Wait())
}

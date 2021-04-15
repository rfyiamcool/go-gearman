package gearman

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gotest.tools/assert"
)

func Test1(t *testing.T) {
	gm := NewGearman(100, 1000)
	gm.Start()
	defer gm.Stop()

	wg := sync.WaitGroup{}
	wg.Add(2)
	cb := func() error {
		wg.Done()
		return nil
	}

	gm.Submit("", cb)
	gm.Submit("", cb)
	wg.Wait()
}

func Test2(t *testing.T) {
	gm := NewGearman(10, 1000)
	gm.Start()
	defer gm.Stop()

	var (
		wg    = sync.WaitGroup{}
		count = 1000000 // 100w
	)

	wg.Add(count)

	cb := func() error {
		wg.Done()
		return nil
	}
	for i := 0; i < count; i++ {
		gm.Submit("", cb)
	}
	wg.Wait()
}

func Test3(t *testing.T) {
	gm := NewGearman(10, 1000)
	gm.Start()
	defer gm.Stop()

	var (
		wg    = sync.WaitGroup{}
		count = 1000000 // 100w
	)

	wg.Add(count)

	cb := func() error {
		wg.Done()
		return nil
	}
	for i := 0; i < count; i++ {
		gm.Submit(fmt.Sprintf("id-%v", i), cb)
	}
	wg.Wait()
}

func Test4(t *testing.T) {
	gincr = 0
	gm := NewGearman(20, 1000)
	gm.Start()

	var (
		wg    = sync.WaitGroup{}
		count = 2000000 // 200w
		tincr int64
	)

	wg.Add(count)

	cb := func() error {
		atomic.AddInt64(&tincr, 1)
		wg.Done()
		return nil
	}
	go func() {
		for i := 0; i < count/2; i++ {
			gm.Submit(fmt.Sprintf("id-%v", i), cb)
		}
	}()
	go func() {
		for i := 0; i < count/2; i++ {
			gm.Submit(fmt.Sprintf("id-%v", i), cb)
		}
	}()
	wg.Wait()
	assert.Equal(t, tincr, int64(count))
	gm.Stop()
	time.Sleep(1 * time.Second)
	assert.Equal(t, int64(20), atomic.LoadInt64(&gincr))
}

func TestSleep(t *testing.T) {
	gm := NewGearman(100, 1000)
	gm.Start()
	defer gm.Stop()

	var (
		wg      = sync.WaitGroup{}
		count   = 100000
		buckets = 5
	)

	// modify globle timeout
	defaultIdleTimeout = time.Duration(100 * time.Microsecond)

	wg.Add(count)
	cb := func() error {
		wg.Done()
		return nil
	}
	for i := 0; i < buckets; i++ {
		go func() {
			for i := 0; i < count/buckets; i++ {
				time.Sleep(time.Duration(rand.Int31n(500)) * time.Microsecond)
				gm.Submit(fmt.Sprintf("id-%v", i), cb)
			}
		}()
	}
	wg.Wait()
}

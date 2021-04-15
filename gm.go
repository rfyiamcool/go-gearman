package gearman

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	defaultIdleTimeout = time.Duration(5 * time.Minute)
)

type worker struct {
	id          int
	ctx         context.Context
	cancel      context.CancelFunc
	running     int32
	queue       chan func() error
	errcall     func(error)
	sleeping    int32
	idleTimeout time.Duration

	sync.RWMutex
}

func newWorker(ctx context.Context, num int) *worker {
	if num <= 0 {
		panic("invalid num")
	}

	cctx, cancel := context.WithCancel(ctx)
	return &worker{
		running: 1,
		queue:   make(chan func() error, num),
		ctx:     cctx,
		cancel:  cancel,
	}
}

func (w *worker) call(cb func() error) {
	err := cb()
	if err != nil {
		w.errcall(err)
	}
}

func (w *worker) start() {
	w.setSleeping(false)

	timer := time.NewTimer(w.idleTimeout)
	for w.isRunning() {
		select {
		case cb := <-w.queue:
			timer.Reset(w.idleTimeout)
			cb()

		case <-w.ctx.Done():
			w.done()
			return

		case <-timer.C:
			w.setSleeping(true)
			return
		}
	}
	w.done()
}

var gincr int64

func (w *worker) done() {
	atomic.AddInt64(&gincr, 1)
}

func (w *worker) enqueue(cb func() error) {
	if w.atomicWaking() {
		go w.start()
	}

	select {
	case w.queue <- cb:
	case <-w.ctx.Done():
	}

	if w.atomicWaking() {
		go w.start()
	}
}

func (w *worker) stop() {
	w.setRunning(false)
	w.cancel()
}

func (w *worker) isRunning() bool {
	return atomic.LoadInt32(&w.running) == 1
}

func (w *worker) setRunning(b bool) {
	if b {
		atomic.StoreInt32(&w.running, 1)
		return
	}

	atomic.StoreInt32(&w.running, 0)
}

func (w *worker) isSleeping() bool {
	return atomic.LoadInt32(&w.sleeping) == 1
}

func (w *worker) setSleeping(b bool) {
	var val int32
	if b {
		val = 1
	}

	atomic.StoreInt32(&w.sleeping, val)
}

func (w *worker) atomicWaking() bool {
	return atomic.CompareAndSwapInt32(&w.sleeping, 1, 0)
}

type optionCall func(*Gearman) error

func SetIdleTimeout(idleTimeout time.Duration) optionCall {
	return func(g *Gearman) error {
		g.idleTimeout = idleTimeout
		return nil
	}
}

func SetExceptionCall(cb func(error)) optionCall {
	return func(g *Gearman) error {
		g.errcall = cb
		return nil
	}
}

type Gearman struct {
	ctx         context.Context
	cancel      context.CancelFunc
	workers     map[int]*worker
	errcall     func(error)
	idleTimeout time.Duration
}

func NewGearmanWithContext(ctx context.Context, wnum, qnum int, options ...optionCall) *Gearman {
	gm := &Gearman{
		workers:     make(map[int]*worker),
		ctx:         ctx,
		idleTimeout: defaultIdleTimeout,
		errcall:     errcall,
	}

	for _, op := range options {
		op(gm)
	}

	for i := 0; i < wnum; i++ {
		w := newWorker(ctx, qnum)
		w.id = i
		w.idleTimeout = gm.idleTimeout
		w.errcall = gm.errcall
		gm.workers[i] = w
	}

	return gm
}

func NewGearman(wnum, qnum int) *Gearman {
	var (
		cctx, cancel = context.WithCancel(context.Background())
	)

	gm := NewGearmanWithContext(cctx, wnum, qnum)
	gm.cancel = cancel
	return gm
}

func (g *Gearman) Start() {
	for _, w := range g.workers {
		go w.start()
	}
}

func (g *Gearman) Stop() {
	if g.cancel != nil {
		g.cancel()
	}
	for _, w := range g.workers {
		w.stop()
	}
}

func (g *Gearman) Submit(key string, cb func() error) error {
	if g.ctx.Err() != nil {
		return errors.New("gearman is already stopped !!!")
	}

	mod := getMod(key, len(g.workers))
	wo := g.workers[mod]
	wo.enqueue(cb)
	return nil
}

func fnv32(key string) int {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return int(hash)
}

func getMod(key string, length int) int {
	num := fnv32(key)
	return num % length
}

func errcall(error) {}

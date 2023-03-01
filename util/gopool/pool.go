// Copyright 2021 ByteDance Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gopool

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/bytedance/gopkg/util/logger"
)

type Pool interface {
	// Name returns the corresponding pool name.
	Name() string
	// SetCap sets the goroutine capacity of the pool.
	SetCap(cap int32)
	// Go executes f.
	Go(f func())
	// CtxGo executes f and accepts the context.
	CtxGo(ctx context.Context, f func())
	// SetPanicHandler sets the panic handler.
	SetPanicHandler(f func(context.Context, interface{}))
	// WorkerCount returns the number of running workers
	WorkerCount() int32
}

var taskPool sync.Pool

func init() {
	taskPool.New = newTask
}

type task struct {
	ctx context.Context
	f   func()

	next *task
}

func (t *task) zero() {
	t.ctx = nil
	t.f = nil
	t.next = nil
}

func (t *task) Recycle() {
	t.zero()
	taskPool.Put(t)
}

func newTask() interface{} {
	return &task{}
}

type taskList struct {
	sync.Mutex
	taskHead *task
	taskTail *task
}

type pool struct {
	// The name of the pool
	name string

	// capacity of the pool, the maximum number of goroutines that are actually working
	cap int32
	// Configuration information
	config *Config
	// linked list of tasks
	taskHead  *task
	taskTail  *task
	taskLock  sync.Mutex
	taskCount int32

	// Record the number of running workers
	workerCount int32

	// This method will be called when the worker panic
	panicHandler func(context.Context, interface{})
}

// NewPool creates a new pool with the given name, cap and config.
func NewPool(name string, cap int32, config *Config) Pool {
	p := &pool{
		name:   name,
		cap:    cap,
		config: config,
	}
	return p
}

func (p *pool) Name() string {
	return p.name
}

func (p *pool) SetCap(cap int32) {
	p.cap = cap
}

func (p *pool) Go(f func()) {
	p.CtxGo(context.Background(), f)
}

func (p *pool) CtxGo(ctx context.Context, f func()) {
	// Try to increase the current num of goroutines, if it is less than the cap,
	// just create a new goroutine directly, no need to enqueue.
	if p.incWorkerCount() < p.cap {
		p.run(ctx, f)
		return
	}
	// Else decrease the num andput the task into the list waiting to be executed.
	// Less lines: for inline opt.
	p.enqueue(ctx, f)
}

func (p *pool) enqueue(ctx context.Context, f func()) {
	p.decWorkerCount()
	t := taskPool.Get().(*task)
	t.ctx = ctx
	t.f = f
	p.taskLock.Lock()
	if p.taskHead == nil {
		p.taskHead = t
		p.taskTail = t
	} else {
		p.taskTail.next = t
		p.taskTail = t
	}
	p.taskLock.Unlock()
}

func (p *pool) run(ctx context.Context, f func()) {
	do := func(ctx context.Context, f func()) {
		defer func() {
			if r := recover(); r != nil {
				if p.panicHandler != nil {
					p.panicHandler(ctx, r)
				} else {
					msg := fmt.Sprintf("GOPOOL: panic in pool: %s: %v: %s", p.name, r, debug.Stack())
					logger.CtxErrorf(ctx, msg)
				}
			}
		}()
		f()
	}

	go func(ctx context.Context, f func()) {
		defer p.decWorkerCount()

		do(ctx, f)
		for {
			var t *task
			p.taskLock.Lock()
			if p.taskHead != nil {
				t = p.taskHead
				p.taskHead = p.taskHead.next
			}
			if t == nil {
				// if there's no task to do, exit
				p.taskLock.Unlock()
				return
			}
			p.taskLock.Unlock()
			do(t.ctx, t.f)
			t.Recycle()
		}
	}(ctx, f)
}

// SetPanicHandler the func here will be called after the panic has been recovered.
func (p *pool) SetPanicHandler(f func(context.Context, interface{})) {
	p.panicHandler = f
}

func (p *pool) WorkerCount() int32 {
	return atomic.LoadInt32(&p.workerCount)
}

func (p *pool) incWorkerCount() int32 {
	return atomic.AddInt32(&p.workerCount, 1)
}

func (p *pool) decWorkerCount() {
	atomic.AddInt32(&p.workerCount, -1)
}

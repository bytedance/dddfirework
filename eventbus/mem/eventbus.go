//
// Copyright 2023 Bytedance Ltd. and/or its affiliates
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

package mem

import (
	"context"
	"sync"

	"github.com/bytedance/dddfirework"
)

type MemoryEventBus struct {
	ch chan *dddfirework.DomainEvent
	cb dddfirework.DomainEventHandler

	once sync.Once
}

func NewEventBus(capacity int) *MemoryEventBus {
	return &MemoryEventBus{
		ch: make(chan *dddfirework.DomainEvent, capacity),
	}
}

func (e *MemoryEventBus) Dispatch(ctx context.Context, evts ...*dddfirework.DomainEvent) error {
	for _, evt := range evts {
		e.ch <- evt
	}
	return nil
}

func (e *MemoryEventBus) RegisterEventHandler(cb dddfirework.DomainEventHandler) {
	e.cb = cb
}

func (e *MemoryEventBus) Start(ctx context.Context) {
	run := func() {
		for {
			select {
			case evt := <-e.ch:
				_ = e.cb(ctx, evt)
			case <-ctx.Done():
				break
			}
		}
	}
	// 确保只启动一次
	e.once.Do(func() {
		go run()
	})
}

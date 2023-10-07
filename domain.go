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

package dddfirework

import (
	"context"
	"reflect"
)

type EntitySlice []IEntity

type IEntity interface {
	IDirty

	SetID(id string)
	GetID() string

	GetChildren() map[string][]IEntity
	GetEvents() []*DomainEvent
}

// IBeforeCreate hook
type IBeforeCreate interface {
	BeforeCreate(ctx context.Context) error
}

type IAfterCreate interface {
	AfterCreate(ctx context.Context) error
}

type IBeforeUpdate interface {
	BeforeUpdate(ctx context.Context) error
}

type IAfterUpdate interface {
	AfterUpdate(ctx context.Context) error
}

type IBeforeDelete interface {
	BeforeDelete(ctx context.Context) error
}

type IAfterDelete interface {
	AfterDelete(ctx context.Context) error
}

type IDirty interface {
	// Dirty 标记实体对象是否需要更新
	Dirty()
	// UnDirty 取消实体的更新标记
	UnDirty()
	// IsDirty 判断实体是否需要更新
	IsDirty() bool
}

type BaseEntity struct {
	id      string
	isDirty bool
	events  []*DomainEvent
}

var entityType = reflect.TypeOf((*IEntity)(nil)).Elem()
var baseEntityType = reflect.TypeOf(&BaseEntity{})

func NewBase(id string) BaseEntity {
	return BaseEntity{id: id}
}

func (e *BaseEntity) SetID(id string) {
	e.id = id
}

func (e *BaseEntity) GetID() string {
	return e.id
}

func (e *BaseEntity) Dirty() {
	e.isDirty = true
}

func (e *BaseEntity) UnDirty() {
	e.isDirty = false
}

func (e *BaseEntity) IsDirty() bool {
	return e.isDirty
}

func (e *BaseEntity) GetChildren() map[string][]IEntity {
	return nil
}

// AddEvent 实体发送事件，调用方需要保证事件是可序列化的，否则会导致 panic
func (e *BaseEntity) AddEvent(evt IEvent, opts ...EventOpt) {
	e.events = append(e.events, NewDomainEvent(evt, opts...))
}

func (e *BaseEntity) GetEvents() []*DomainEvent {
	return e.events
}

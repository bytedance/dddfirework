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
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/rs/xid"
)

var ErrNoEventBusFound = fmt.Errorf("no eventbus found")

type EventType string

// SendType 事件发送类型
type SendType string

const (
	SendTypeNormal      SendType = "normal"      // 普通事件
	SendTypeFIFO        SendType = "FIFO"        // 保序事件,即事件以 Sender 的发送时间顺序被消费执行,且前序事件执行失败时会阻塞后续事件执行
	SendTypeLaxFIFO     SendType = "LaxFIFO"     // 保序事件,即事件以 Sender 的发送时间顺序被消费执行,但前序事件的执行成败不影响后续事件
	SendTypeTransaction SendType = "transaction" // 事务事件
	SendTypeDelay       SendType = "delay"       // 延时发送
)

type EventOption struct {
	SendType SendType
	SendTime time.Time // 设定发送时间
}

type EventOpt func(opt *EventOption)

func WithSendType(t SendType) EventOpt {
	return func(opt *EventOption) {
		opt.SendType = t
	}
}

type TXStatus int

const (
	TXUnknown  TXStatus = 0 // 未知状态，EventBus 应当支持未知的重试
	TXCommit   TXStatus = 1 // 提交事务
	TXRollBack TXStatus = 2 // 回滚事务
)

// EventHandler 指定特定领域事件的事件处理器，必须是带有 2 个入参的函数类型，第一个参数为 context.Context 类型
// 第二个为与 EventType 匹配的事件数据指针类型， 示例 func(ctx context.Context, evt *OrderCreatedEvent) error
// 当第二个参数声明为 *DomainEvent，EventHandler 回调时会传入原始的事件类型，用户定义事件以序列化形式存在 DomainEvent.Payload 中
type EventHandler interface{}

// DomainEventHandler 通用 DomainEvent 的事件处理器
type DomainEventHandler func(ctx context.Context, evt *DomainEvent) error

// EventTXChecker 事务事件的回查接口，必须带 1 个事件入参，1 个 TXStatus 返回值的函数
// 示例：func(evt *OrderCreatedEvent) TXStatus
type EventTXChecker interface{}

// DomainEventTXChecker 通用 DomainEvent 的事务回查
type DomainEventTXChecker func(evt *DomainEvent) TXStatus

var eventBusMu sync.Mutex
var eventRouter = map[EventType][]*eventHandler{}
var eventTXCheckerRouter = map[EventType]*eventHandler{}

type eventHandler struct {
	f         reflect.Value // the actual callback function
	eventType reflect.Type  // 存储实际事件类型
}

var ctxType = reflect.TypeOf((*context.Context)(nil)).Elem()
var errType = reflect.TypeOf((*error)(nil)).Elem()
var domainEventType = reflect.TypeOf(DomainEvent{})
var txStatusType = reflect.TypeOf(TXUnknown)

// RegisterEventHandler 注册事件处理器
func RegisterEventHandler(t EventType, handler EventHandler) {
	eventBusMu.Lock()
	defer eventBusMu.Unlock()

	handlerType := reflect.TypeOf(handler)
	if handlerType.Kind() != reflect.Func {
		panic("handler must type of reflect.Func")
	}
	if handlerType.NumIn() != 2 || !handlerType.In(0).Implements(ctxType) {
		panic("handler must has 2 args and the first must be type of context.Context")
	}
	if handlerType.NumOut() != 1 || !handlerType.Out(0).Implements(errType) {
		panic("handler must has error as output")
	}
	argType := handlerType.In(1)
	if argType.Kind() != reflect.Ptr {
		panic("event type must be pointer")
	}
	eventType := argType.Elem()
	eventRouter[t] = append(eventRouter[t], &eventHandler{
		f:         reflect.ValueOf(handler),
		eventType: eventType,
	})
}

func hasEventHandler(t EventType) bool {
	eventBusMu.Lock()
	defer eventBusMu.Unlock()

	_, ok := eventRouter[t]
	return ok
}

// RegisterEventTXChecker 注册事务反查接口
func RegisterEventTXChecker(t EventType, checker EventTXChecker) {
	eventBusMu.Lock()
	defer eventBusMu.Unlock()

	if eventTXCheckerRouter[t] != nil {
		panic(fmt.Sprintf("event tx checker of type(%s) repeated!", t))
	}

	handlerType := reflect.TypeOf(checker)
	if handlerType.Kind() != reflect.Func {
		panic("handler must type of reflect.Func")
	}
	if handlerType.NumIn() != 1 {
		panic("handler must has 1 args and the first must be type of context.Context")
	}
	if handlerType.NumOut() != 1 || !handlerType.Out(0).ConvertibleTo(txStatusType) {
		panic("handler must 1 output type of TXStatus")
	}
	argType := handlerType.In(0)
	if argType.Kind() != reflect.Ptr {
		panic("event type must be pointer")
	}
	eventType := argType.Elem()
	eventTXCheckerRouter[t] = &eventHandler{
		f:         reflect.ValueOf(checker),
		eventType: eventType,
	}
}

// RegisterEventBus 注册事件总线
func RegisterEventBus(eventBus IEventBus) {
	eventBus.RegisterEventHandler(onEvent)
	if txEventBus, ok := eventBus.(ITransactionEventBus); ok {
		txEventBus.RegisterEventTXChecker(onTXChecker)
	}
}

// onEvent EventBus 的统一的回调入口
func onEvent(ctx context.Context, evt *DomainEvent) error {
	defaultLogger.Info("on event call", "event", evt)
	eventBusMu.Lock()
	handlers := eventRouter[evt.Type]
	eventBusMu.Unlock()

	for _, h := range handlers {
		var bizEvt reflect.Value
		if h.eventType == domainEventType {
			bizEvt = reflect.ValueOf(evt)
		} else {
			bizEvt = reflect.New(h.eventType)
			if err := json.Unmarshal(evt.Payload, bizEvt.Interface()); err != nil {
				return err
			}
		}

		outs := h.f.Call([]reflect.Value{reflect.ValueOf(ctx), bizEvt})
		if !outs[0].IsNil() {
			return outs[0].Interface().(error)
		}
	}

	return nil
}

func onTXChecker(evt *DomainEvent) TXStatus {
	eventBusMu.Lock()
	checker := eventTXCheckerRouter[evt.Type]
	eventBusMu.Unlock()

	// 未注册回查，对不不确定的结果，默认回滚
	if checker == nil {
		return TXRollBack
	}

	var bizEvt reflect.Value
	if checker.eventType == domainEventType {
		bizEvt = reflect.ValueOf(evt)
	} else {
		bizEvt = reflect.New(checker.eventType)
		if err := json.Unmarshal(evt.Payload, bizEvt.Interface()); err != nil {
			return TXRollBack
		}
	}

	outs := checker.f.Call([]reflect.Value{bizEvt})
	return outs[0].Interface().(TXStatus)
}

type IEvent interface {
	GetType() EventType // 事件类型
	GetSender() string  // 发送者id，可以用来实现事件保序
}

type DomainEvent struct {
	ID        string
	Type      EventType
	SendType  SendType
	Sender    string // 事件发出实体 ID
	Payload   []byte
	CreatedAt time.Time
}

func (d *DomainEvent) GetType() EventType {
	return d.Type
}

func (d *DomainEvent) GetSender() string {
	return d.Sender
}

func NewDomainEvent(event IEvent, opts ...EventOpt) *DomainEvent {
	opt := &EventOption{
		SendType: SendTypeNormal,
	}
	for _, o := range opts {
		o(opt)
	}
	bs, err := json.Marshal(event)
	if err != nil {
		panic(fmt.Sprintf("event marshal failed, err=%s", err))
	}
	return &DomainEvent{
		ID:        xid.New().String(),
		Type:      event.GetType(),
		SendType:  opt.SendType,
		Sender:    event.GetSender(),
		Payload:   bs,
		CreatedAt: time.Now(),
	}
}

type IEventBus interface {
	// Dispatch 发送领域事件到 EventBus，该方法会在事务内被同步调用
	// 对于每个事件，EventBus 必须要至少保证 at least once 送达
	Dispatch(ctx context.Context, evt ...*DomainEvent) error

	// RegisterEventHandler 注册事件回调，IEventBus 的实现必须保证收到事件同步调用该回调
	RegisterEventHandler(cb DomainEventHandler)
}

// ITransactionEventBus 支持事务性消息的实现
// 如果 eventbus 实现了这个接口，当发送事件的 DomainEvent.SendType == SendTypeTransaction 会优先选择事务的方式来发送
type ITransactionEventBus interface {
	IEventBus

	// RegisterEventTXChecker 注册事件回查
	RegisterEventTXChecker(cb DomainEventTXChecker)

	// DispatchBegin 开始发送事务，eventbus 需要保证同步成功，但是暂不发送到消费端
	// 返回值 context.Context 会传递给 Commie 和 Rollback 接口，实现方可以利用机制传递上下文信息
	DispatchBegin(ctx context.Context, evt ...*DomainEvent) (context.Context, error)

	// Commit 发送事件到消费端
	Commit(ctx context.Context) error

	// Rollback 回滚事件
	Rollback(ctx context.Context) error
}

type noEventBus struct {
}

func (d *noEventBus) Dispatch(ctx context.Context, evt ...*DomainEvent) error {
	return ErrNoEventBusFound
}

func (d *noEventBus) PostSave(ctx context.Context, commitOrRollback bool) {}

func (d *noEventBus) RegisterEventHandler(cb DomainEventHandler) {

}

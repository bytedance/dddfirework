package dddfirework

import (
	"context"
	"fmt"
	"time"
)

var ErrNoEventTimerFound = fmt.Errorf("no event_timer found")

type TimerHandler func(ctx context.Context, key, cron string, data []byte) error

// ITimer 分布式定时器协议
type ITimer interface {
	// RegisterTimerHandler 注册定时任务，定时到来时候调用该回调函数
	RegisterTimerHandler(cb TimerHandler)

	// RunCron 按照 cron 语法设置定时，并在定时到达后作为参数调用定时任务回调
	// key: 定时任务唯一标识，重复调用时不覆盖已有计时; cron: 定时配置; data: 透传数据，回调函数传入
	RunCron(key, cron string, data []byte) error

	// RunOnce 指定时间单次运行
	// key: 定时任务唯一标识，重复调用时不覆盖已有计时; t: 执行时间; data: 透传数据，回调函数传入
	RunOnce(key string, t time.Time, data []byte) error

	// Cancel 删除某个定时
	Cancel(key string) error
}

type noTimer struct {
}

func (d *noTimer) RunCron(key, cron string, data []byte) error {
	return ErrNoEventTimerFound
}

func (d *noTimer) RunOnce(key string, t time.Time, data []byte) error {
	return ErrNoEventTimerFound
}

func (d *noTimer) RegisterTimerHandler(cb TimerHandler) {
}

func (d *noTimer) Cancel(key string) error {
	return nil
}

// TimerEvent 定时器专用的事件
type TimerEvent struct {
	Key     string
	Cron    string
	Payload []byte
}

func (e *TimerEvent) GetType() EventType {
	return EventType(e.Key)
}

func (e *TimerEvent) GetSender() string {
	return ""
}

func onTimer(ctx context.Context, key, cron string, data []byte) error {
	return onEvent(ctx, NewDomainEvent(&TimerEvent{Key: key, Cron: cron, Payload: data}))
}

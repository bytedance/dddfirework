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

package mysql

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/go-logr/logr"
	"github.com/robfig/cron"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/bytedance/dddfirework"
	"github.com/bytedance/dddfirework/logger"
	"github.com/bytedance/dddfirework/logger/stdr"
)

const retryInterval = time.Second * 3
const retryLimit = 5
const runInterval = time.Millisecond * 100

// 每天凌晨两点执行clean
const cleanCron = "0 2 * * *"

// 消费完成的event保留一段时间以便追查问题
const retentionTime = 48 * time.Hour
const consumeConcurrent = 1          // 事件处理并发数
const limitPerRun = 100              // 单次 handleEvents 处理的事件数
const scanStartTime = time.Hour * 24 // 事件扫描最大起始时间
const txCheckTimeout = time.Minute

var ErrInvalidDB = fmt.Errorf("invalid db")
var ErrNoTransaction = fmt.Errorf("no transaction")
var ErrServiceNotCreate = fmt.Errorf("service not create")
var ErrPrecedingEventNotReady = fmt.Errorf("preceding event not ready")

var defaultLogger = stdr.NewStdr("mysql_eventbus")
var eventBusMu sync.Mutex

type IRetryStrategy interface {
	// Next 获取下一次重试的策略，返回 nil 表示不再重试
	// 当 RetryInfo.RetryCount == 0 表示初始化状态，通过 Next 获取第一次重试信息
	Next(info *RetryInfo) *RetryInfo
	RetryCount() int
}

// LimitRetry 设定最大重试次数，不指定间隔
type LimitRetry struct {
	Limit int
}

func (c *LimitRetry) Next(info *RetryInfo) *RetryInfo {
	if info.RetryCount > c.Limit {
		return nil
	}
	return &RetryInfo{
		ID:         info.ID,
		RetryCount: info.RetryCount + 1,
		RetryTime:  time.Now(),
	}
}
func (c *LimitRetry) RetryCount() int {
	return c.Limit
}

// IntervalRetry 指定固定间隔和次数
type IntervalRetry struct {
	Interval time.Duration
	Limit    int
}

func (c *IntervalRetry) Next(info *RetryInfo) *RetryInfo {
	if info.RetryCount > c.Limit {
		return nil
	}
	lastTime := info.RetryTime
	if info.RetryCount == 0 {
		lastTime = time.Now()
	}
	return &RetryInfo{
		ID:         info.ID,
		RetryCount: info.RetryCount + 1,
		RetryTime:  lastTime.Add(c.Interval),
	}
}
func (c *IntervalRetry) RetryCount() int {
	return c.Limit
}

// CustomRetry 自定义重试次数和间隔
type CustomRetry struct {
	Intervals []time.Duration
}

func (c *CustomRetry) Next(info *RetryInfo) *RetryInfo {
	if info.RetryCount >= len(c.Intervals) {
		return nil
	}
	lastTime := info.RetryTime
	if info.RetryCount == 0 {
		lastTime = time.Now()
	}
	return &RetryInfo{
		ID:         info.ID,
		RetryCount: info.RetryCount + 1,
		RetryTime:  lastTime.Add(c.Intervals[info.RetryCount]),
	}
}
func (c *CustomRetry) RetryCount() int {
	return len(c.Intervals)
}

type Options struct {
	// 重试策略：有两种方式
	// 1, RetryInterval + RetryLimit 表示固定间隔重试
	// 2, CustomRetry 表示自定义间隔重试
	RetryLimit    int             // 重试次数
	RetryInterval time.Duration   // 重试间隔
	CustomRetry   []time.Duration // 自定义重试间隔

	DefaultOffset     *int64        // 默认起始 offset
	RunInterval       time.Duration // 默认轮询间隔
	CleanCron         string        // 默认清理周期
	RetentionTime     time.Duration // 消费完成的event在db里的保留时间
	LimitPerRun       int           // 每次轮询最大的处理条数
	ConsumeConcurrent int           // 事件消费的并发数
	RetryStrategy     IRetryStrategy
	TXCheckTimeout    time.Duration
	Logger            logr.Logger
}

type Option func(opt *Options)
type contextKey string

type EventBus struct {
	serviceName   string
	db            *gorm.DB
	logger        logr.Logger
	opt           Options
	retryStrategy IRetryStrategy
	// 事务结果反查，在事务结果（提交 or 回滚）超时未被调用，会通过反查接口询问结果
	// 当前仅通过简单的定时来实现，可靠性依赖所在组件的正常运行
	txChecker dddfirework.DomainEventTXChecker
	txKey     contextKey

	cb        dddfirework.DomainEventHandler
	cleanCron *cron.Cron
	once      sync.Once
}

// NewEventBus 提供领域事件直接持久化到数据库，异步查询事件并推送的功能
// 需要在业务数据库提前创建符合 EventPO, ServicePO 描述的库表，并且使用兼容 gorm Model 的 executor
// 参数：serviceName 服务名，同一个服务之间只有一个消费，不同服务之间独立消费
// 用法：eventBus := NewEventBus("service", db); NewEngine(lock, eventBus.Options()...)
func NewEventBus(serviceName string, db *gorm.DB, options ...Option) *EventBus {
	if utf8.RuneCountInString(serviceName) > 30 {
		panic("serviceName must less than 30 chars")
	}

	opt := Options{
		RunInterval:       runInterval,
		CleanCron:         cleanCron,
		RetentionTime:     retentionTime,
		ConsumeConcurrent: consumeConcurrent,
		LimitPerRun:       limitPerRun,
		TXCheckTimeout:    txCheckTimeout,
		Logger:            defaultLogger,
	}
	for _, o := range options {
		o(&opt)
	}
	if _, err := cron.Parse(opt.CleanCron); err != nil {
		panic(fmt.Sprintf("cron expression %s is invalid", opt.CleanCron))
	}
	if opt.RetentionTime < 0 {
		panic(fmt.Sprintf("retentionTime %v can not be negative", opt.RetentionTime))
	}
	var strategy IRetryStrategy
	if opt.RetryStrategy != nil {
		strategy = opt.RetryStrategy
	} else if opt.RetryInterval > 0 {
		strategy = &IntervalRetry{Interval: opt.RetryInterval, Limit: opt.RetryLimit}
	} else if len(opt.CustomRetry) > 0 {
		strategy = &CustomRetry{Intervals: opt.CustomRetry}
	} else {
		strategy = &IntervalRetry{Interval: retryInterval, Limit: retryLimit}
	}

	eb := &EventBus{
		serviceName:   serviceName,
		db:            db,
		logger:        opt.Logger,
		retryStrategy: strategy,
		opt:           opt,
		txKey:         contextKey(fmt.Sprintf("eventbus_tx_%d", time.Now().Unix())),
		cleanCron:     cron.New(),
	}
	_ = eb.initService()
	return eb
}

func (e *EventBus) Options() []dddfirework.Option {
	return []dddfirework.Option{
		dddfirework.WithEventBus(e),
		dddfirework.WithPostSave(e.onPostSave),
	}
}

func (e *EventBus) ctxWithDB(ctx context.Context, db *gorm.DB) context.Context {
	return context.WithValue(ctx, e.txKey+":db", db)
}

// getDB 获取上下文中的 db 句柄，有事务场景下调用的，必须用该方法获取 DB
func (e *EventBus) getDB(ctx context.Context) *gorm.DB {
	val := ctx.Value(e.txKey + ":db")
	if val != nil {
		return val.(*gorm.DB)
	}

	return e.db
}

func (e *EventBus) ctxWithTX(ctx context.Context, tx *Transaction) context.Context {
	return context.WithValue(ctx, e.txKey+":tx", tx)
}

func (e *EventBus) getTX(ctx context.Context) *Transaction {
	val := ctx.Value(e.txKey + ":tx")
	if val != nil {
		return val.(*Transaction)
	}

	return nil
}

// Dispatch ...
func (e *EventBus) Dispatch(ctx context.Context, events ...*dddfirework.DomainEvent) error {
	tx := e.getTX(ctx)
	pos := make([]*EventPO, len(events))

	for i, evt := range events {
		po, err := eventPersist(evt)
		if err != nil {
			return err
		}
		if tx != nil {
			po.TransID = tx.ID
		}
		pos[i] = po
	}
	// return e.getDB(ctx).Create(pos).Error
	// 在创建event的时候顺带创建service_event，在消费时创建service_event 非常复杂，必须联表查询容易导致慢sql 问题，且容易漏建service_event。
	return e.db.Transaction(func(tx *gorm.DB) error {
		if err := e.getDB(ctx).Create(pos).Error; err != nil {
			return err
		}
		// gorm 在创建po后为对象赋值主键自增id
		serviceNames, err := e.listService()
		if err != nil {
			return err
		}
		for _, serviceName := range serviceNames {
			spos := make([]*ServiceEventPO, len(events))
			for i, po := range pos {
				spos[i] = &ServiceEventPO{
					Service:        serviceName,
					EventID:        po.ID,
					Status:         ServiceEventStatusInit,
					EventCreatedAt: po.EventCreatedAt,
					// 初始化时给一个尽量早的可执行时间，表示创建后就可以执行了
					NextTime: po.EventCreatedAt,
				}
			}
			if err := e.getDB(ctx).Create(spos).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func (e *EventBus) RegisterEventTXChecker(checker dddfirework.DomainEventTXChecker) {
	e.txChecker = checker
}

// DispatchBegin 开启事务消息
func (e *EventBus) DispatchBegin(ctx context.Context, evts ...*dddfirework.DomainEvent) (context.Context, error) {
	if len(evts) == 0 {
		return ctx, fmt.Errorf("events can not be empty")
	}
	tx := &Transaction{
		Service: e.serviceName,
		Events:  evts,
		DueTime: time.Now().Add(e.opt.TXCheckTimeout),
	}
	if err := e.getDB(ctx).Create(tx).Error; err != nil {
		return ctx, err
	}
	return e.ctxWithTX(ctx, tx), nil
}

// Commit 提交事务消息
func (e *EventBus) Commit(ctx context.Context) error {
	tx := e.getTX(ctx)
	if tx == nil {
		return ErrNoTransaction
	}

	err := e.getDB(ctx).Where("trans_id = ?", tx.ID).First(&EventPO{}).Error
	if err != gorm.ErrRecordNotFound {
		return err
	}
	if err == gorm.ErrRecordNotFound {
		err := e.Dispatch(ctx, tx.Events...)
		if err != nil {
			return err
		}
	}

	return e.getDB(ctx).Delete(tx).Error
}

// Rollback 回滚事务消息
func (e *EventBus) Rollback(ctx context.Context) error {
	tx := e.getTX(ctx)
	if tx == nil {
		return ErrNoTransaction
	}
	return e.getDB(ctx).Delete(tx).Error
}

func (e *EventBus) onPostSave(ctx context.Context, res *dddfirework.Result) {
	go func() {
		_ = e.handleEvents()
	}()
}

func (e *EventBus) RegisterEventHandler(cb dddfirework.DomainEventHandler) {
	e.cb = cb
}

func (e *EventBus) initService() error {
	service := &ServicePO{}
	return e.db.Where(ServicePO{Name: e.serviceName}).FirstOrCreate(service).Error
}

func (e *EventBus) listService() ([]string, error) {
	services := make([]*ServicePO, 0)
	if err := e.db.Find(&services).Error; err != nil {
		return nil, err
	}
	serviceNames := make([]string, 0)
	for _, service := range services {
		serviceNames = append(serviceNames, service.Name)
	}
	return serviceNames, nil
}

type EventTuple struct {
	ServiceEvent *ServiceEventPO
	Event        *EventPO
}

// getScanEvents 扫描待处理的event：status=init and 到达下次可执行时间的event
func (e *EventBus) getScanEvents(scanStartEventID int64) ([]*EventTuple, error) {
	eventOffset := scanStartEventID
	// 从service_event 视角查询最早的 需要的处理的event。通过计算 eventOffset来提高查询效率
	earliestRunnableServiceEvent := &ServiceEventPO{}
	if err := e.db.Where("service = ?", e.serviceName).
		Where("status = ?", ServiceEventStatusInit).
		Where("event_id >= ?", scanStartEventID).
		Order("event_id").Take(earliestRunnableServiceEvent).Error; err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, err
		}
		return nil, nil
	}
	if earliestRunnableServiceEvent.ID > 0 {
		if eventOffset < earliestRunnableServiceEvent.ID {
			eventOffset = earliestRunnableServiceEvent.EventID
		}
	}
	//// 每次只扫描 event.id=[eventOffset,eventBound) 区间内的event，防止未消费event 过多时导致的慢sql，
	//eventBound := eventOffset + int64(e.opt.LimitPerRun)
	////eventBound是在eventOffset 超过重试次数之前，最大可以处理到的event
	//if e.retryStrategy.RetryCount() > 1 {
	//	eventBound = eventOffset + int64(e.opt.LimitPerRun*e.retryStrategy.RetryCount())
	//}
	serviceEventPOs := make([]*ServiceEventPO, 0)
	if err := e.db.Where("service = ?", e.serviceName).
		Where("status = ?", ServiceEventStatusInit).
		Where("event_id >= ?", eventOffset).
		//Where("event_id < ?", eventBound).
		Where("next_time <= ?", time.Now()).
		Order("event_id").Limit(e.opt.LimitPerRun).Find(&serviceEventPOs).Error; err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, err
		}
		return nil, nil
	}
	eventIds := make([]int64, 0)
	for _, serviceEvent := range serviceEventPOs {
		eventIds = append(eventIds, serviceEvent.EventID)
	}
	eventPOs := make([]*EventPO, 0)
	if err := e.db.Where("id in ?", eventIds).Order("id").Find(&eventPOs).Error; err != nil {
		return nil, err
	}
	// eventPO与serviceEventPO 都按event id排序才能进行下一步
	events := make([]*EventTuple, 0)
	for index, eventPO := range eventPOs {
		events = append(events, &EventTuple{
			ServiceEvent: serviceEventPOs[index],
			Event:        eventPO,
		})
	}
	return events, nil
}

func (e *EventBus) doRetryStrategy(spo *ServiceEventPO) {
	// 没有定义重试策略，默认不重试直接失败
	if e.retryStrategy == nil {
		spo.Status = ServiceEventStatusFailed
		return
	}
	info := &RetryInfo{
		ID:         spo.EventID,
		RetryCount: spo.RetryCount,
		RetryTime:  time.Now(),
	}
	newInfo := e.retryStrategy.Next(info)
	if newInfo != nil {
		spo.RetryCount = newInfo.RetryCount
		spo.NextTime = newInfo.RetryTime
	} else {
		spo.Status = ServiceEventStatusFailed
	}

	return
}

func (e *EventBus) dispatchEvents(ctx context.Context, eventPOs []*EventTuple, scanStartEventID int64) {
	eventChan := make(chan *EventTuple, len(eventPOs))
	for _, e := range eventPOs {
		eventChan <- e
	}
	close(eventChan)

	wg := sync.WaitGroup{}
	for i := 0; i < e.opt.ConsumeConcurrent; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cb := func(ctx context.Context, po *EventPO, spo *ServiceEventPO) {
				// 某个event 的处理成败，不影响后续事件的处理
				// 对于保序事件，因为前序事件没有执行，后面同sender event也不用继续了。但毕竟后续还有非保序事件要处理，所以也不会因为ErrPrecedingEventNotReady 就终止事件消费流程
				_ = e.handleEvent(ctx, po, spo, scanStartEventID)
			}
			for po := range eventChan {
				cb(ctx, po.Event, po.ServiceEvent)
			}
		}()
	}
	wg.Wait()
	return
}

func (e *EventBus) checkPrecedingEvent(tx *gorm.DB, eventPO *EventPO, spo *ServiceEventPO, scanStartEventID int64) error {
	// 保序event 要求sender 不能为空
	if len(eventPO.Sender) == 0 {
		err := fmt.Errorf("event sender can not be empty when event type is fifo")
		e.logger.Error(err, "event_id", spo.EventID)
		return err
	}
	// 找到前序event
	precedingEvent := &EventPO{}
	if err := tx.Where("sender = ?", eventPO.Sender).
		// event_created_at 是最权威的前序，但是时间精度问题导致可能前序event.event_created_at可能跟当前event一样，再用event_id 明确下
		Where("event_created_at <= ?", eventPO.EventCreatedAt).
		Where("id < ?", eventPO.ID).
		Order("event_created_at desc, event_id desc").Take(precedingEvent).Error; err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			e.logger.V(logger.LevelInfo).Info("find preceding event error", "current event_id", spo.EventID, "err", err)
			return err
		}
	}
	// 找不到前序event，则当前 event 可能是同sender的第一个event
	if precedingEvent.ID == 0 {
		e.logger.V(logger.LevelInfo).Info("can not find preceding event, see it as first event", "current event_id", spo.EventID)
		return nil
	} else if precedingEvent.ID < scanStartEventID {
		// 如果堵塞的event在event保留期内未被处理成功，在scanStartEventID之前的service_event不会被扫描到，也永远不会被处理，此时放弃它们，不要导致后续的event 处理堵塞。
		e.logger.V(logger.LevelInfo).Info("preceding event is expired, see it as first event", "current event_id", spo.EventID)
		return nil
	}
	// 找到前序service_event
	precedingServiceEvent := &ServiceEventPO{}
	if err := tx.Where("service = ?", e.serviceName).
		Where("event_id = ?", precedingEvent.ID).
		First(precedingServiceEvent).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			e.logger.V(logger.LevelInfo).Info("can not find preceding service_event, see it as first event", "current event_id", spo.EventID)
			return nil
		}
		e.logger.V(logger.LevelInfo).Info("find preceding service_event error", "current event_id", spo.EventID, "err", err)
		return err
	}
	if eventPO.Event.SendType == dddfirework.SendTypeFIFO {
		if precedingServiceEvent.Status != ServiceEventStatusSuccess && precedingServiceEvent.Status != ServiceEventStatusExpired {
			// 前序event 未执行、失败、未过期，则不执行当前event
			e.logger.V(logger.LevelInfo).Info("preceding service_event is not success or expired, ignore", "current event_id", spo.EventID)
			return ErrPrecedingEventNotReady
		}
	} else if eventPO.Event.SendType == dddfirework.SendTypeLaxFIFO {
		// 当期的策略是，哪怕前序event 在重试中，也还是init，除非超过重试次数status=failed。若想忽略重试这一点，可以check下 precedingServiceEvent.retryCount
		if precedingServiceEvent.Status == ServiceEventStatusInit {
			e.logger.V(logger.LevelInfo).Info("find preceding service_event has not been run", "current event_id", spo.EventID)
			// 前序event 未执行，则不执行当前event
			return ErrPrecedingEventNotReady
		}
	}
	e.logger.V(logger.LevelInfo).Info("preceding event is ready", "current event_id", spo.EventID,
		"preceding event_id", precedingServiceEvent.EventID,
		"precedingServiceEvent status", precedingServiceEvent.Status)
	return nil
}

// handleEvent, 多实例并发场景下先锁住service_event，如果是保序event，则check前序event状态，check通过后，执行event handler
// 执行成功，则保存执行结果，执行失败则根据RetryStrategy更新retryCount/nextTime/failedMessage
func (e *EventBus) handleEvent(ctx context.Context, po *EventPO, spo *ServiceEventPO, scanStartEventID int64) error {
	// 下面的db 操作一定要全部使用 tx
	fn := func(tx *gorm.DB) (err error) {
		// 找到并锁住serviceEvent，只能锁service_event。如果锁event，则其他service在消费event也会尝试锁event
		currentServiceEvent := &ServiceEventPO{}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "NOWAIT"}).First(currentServiceEvent, spo.ID).Error; err != nil {
			e.logger.Info("find and lock current service_event error", "current event_id", po.ID, "err", err)
			return err
		}
		// 二次确认下service_event 未被处理
		if spo.Status != ServiceEventStatusInit {
			e.logger.V(logger.LevelInfo).Info("current service_event'status it not init, ignore", "event_id", spo.EventID)
			return nil
		}
		defer func() {
			if r := recover(); r != nil {
				err := fmt.Errorf("err: %v stack:%s", r, string(debug.Stack()))
				e.logger.Error(err, fmt.Sprintf("panic while handling event(%d)", spo.EventID))
				e.doRetryStrategy(spo)
				spo.FailedMessage = fmt.Sprintf("%v", err)
			}
			// 65535 是FailedMessage 字段的db类型长度
			if len(spo.FailedMessage) > 65535 {
				spo.FailedMessage = spo.FailedMessage[:65535]
			}
			spo.RunAt = time.Now()
			// 插入或更新 service_event。 此处必须覆盖掉err 的值，目的是保证service_event 一定进到db
			if err = tx.Save(spo).Error; err != nil {
				e.logger.Error(err, "create or update service_event error", "current event_id", spo.EventID)
				return
			}
		}()

		// 如果是保序event，则校验前序event的执行结果
		if po.Event.SendType == dddfirework.SendTypeFIFO || po.Event.SendType == dddfirework.SendTypeLaxFIFO {
			if err = e.checkPrecedingEvent(tx, po, spo, scanStartEventID); err != nil {
				spo.FailedMessage = fmt.Sprintf("%v", err)
				return err
			}
		}
		e.logger.V(logger.LevelInfo).Info("eventbus handle event", "event db id", spo.EventID, "event id", po.EventID)
		err = e.cb(ctx, po.Event)
		if err != nil {
			// 更新 retry_limit 以及 next_time，超出重试限制，则status置为失败。为了支持自定义RetryStrategy，所以通过NextTime 而不是RunAt来控制重试间隔
			e.doRetryStrategy(spo)
			spo.FailedMessage = fmt.Sprintf("%v", err)
		} else {
			spo.Status = ServiceEventStatusSuccess
		}
		return nil
	}
	return e.db.Transaction(fn)
}

func (e *EventBus) handleEvents() error {
	e.logger.Info("handle events start")
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("err: %v stack:%s", r, string(debug.Stack()))
			e.logger.Error(err, "panic while handleEvents")
		}
	}()
	ctx := context.Background()
	curScanStartTime := time.Now().Add(-scanStartTime)
	// 根据 curScanStartTime确定扫描起点
	scanStartEvent := &EventPO{}
	if err := e.db.Where("event_created_at >= ?", curScanStartTime).First(scanStartEvent).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		return err
	}
	scanEvents, err := e.getScanEvents(scanStartEvent.ID)
	if err != nil {
		e.logger.Error(err, "getScanEvents error")
		return err
	}
	if len(scanEvents) == 0 {
		e.logger.Info("find empty service_event, ignore")
		return nil
	}
	e.dispatchEvents(ctx, scanEvents, scanStartEvent.ID)
	e.logger.Info("handle events end")
	return nil
}

func (e *EventBus) checkTX(ctx context.Context, tx *Transaction) {
	ctx = e.ctxWithTX(ctx, tx)
	evt := tx.Events[0]
	st := e.txChecker(evt)
	if st == dddfirework.TXCommit {
		if err := e.Commit(ctx); err != nil {
			e.logger.Error(err, "[eventbus] commit failed after tx check timeout")
		}
	} else if st == dddfirework.TXRollBack {
		if err := e.Rollback(ctx); err != nil {
			e.logger.Error(err, "[eventbus] rollback failed after tx check timeout")
		}
	}
}

func (e *EventBus) handleTransactions() error {
	return e.db.Transaction(func(db *gorm.DB) error {
		ctx := e.ctxWithDB(context.TODO(), db)

		trans := make([]*Transaction, 0)
		if err := db.Clauses(clause.Locking{Strength: "UPDATE", Options: "NOWAIT"}).Where(
			"service = ? and due_time < ?", e.serviceName, time.Now(),
		).Find(&trans).Error; err != nil {
			return err
		}

		for _, tx := range trans {
			e.checkTX(ctx, tx)
		}
		return nil
	})
}

// cleanEvents 先清理当前service的service_event，如果一个event 被所有service_event 都处理成功，则删除对应的event
func (e *EventBus) cleanEvents() error {
	e.logger.Info("clean events")
	// 遍历可以清理的service_event，加上for update 防止多实例场景下并发操作问题
	query := e.db.Model(&ServiceEventPO{}).Clauses(clause.Locking{Strength: "UPDATE"})
	needCleanAt := time.Now().Add(-e.opt.RetentionTime)
	// 清理成功service_event
	query = query.Where("service = ?", e.serviceName).Where("status = ?", ServiceEventStatusSuccess).Where("event_created_at < ?", needCleanAt)
	rows, err := query.Rows()
	if err != nil {
		return err
	}
	defer func() {
		_ = rows.Close()
	}()

	type EventIDCount struct {
		EventID int64 `json:"event_id"`
		Count   int   `json:"count"`
	}

	// 批量删除service_event
	deleteServiceEvents := func(eventIDs []int64) error {
		if err := e.db.Where("service = ?", e.serviceName).Where("event_id in ?", eventIDs).Delete(ServiceEventPO{}).Error; err != nil {
			return err
		}
		// 假设存在多个service，经过多个service的陆续操作，则已经成功的service_event 陆续被清理，如果service_event 已无该event_id 的记录，则清理该event记录
		var eventIDCounts []EventIDCount
		if err := e.db.Model(&ServiceEventPO{}).Select("event_id, count(id) as count").
			Where("event_id in ?", eventIDs).
			Group("event_id").Find(&eventIDCounts).Error; err != nil {
			return err
		}
		// 能查到说明 event 还在被其它service_event引用
		reservedEventIDs := map[int64]bool{}
		for _, eventIDCount := range eventIDCounts {
			reservedEventIDs[eventIDCount.EventID] = true
		}
		deleteEventIDs := make([]int64, 0)
		for _, eventID := range eventIDs {
			if !reservedEventIDs[eventID] {
				deleteEventIDs = append(deleteEventIDs, eventID)
			}
		}
		if len(deleteEventIDs) == 0 {
			return nil
		}
		if err := e.db.Where("id in ?", deleteEventIDs).Delete(EventPO{}).Error; err != nil {
			return err
		}
		return nil
	}
	batch := 10
	eventIDs := make([]int64, 0)
	for rows.Next() {
		serviceEvent := &ServiceEventPO{}
		err = e.db.ScanRows(rows, serviceEvent)
		if err != nil {
			return err
		}
		eventIDs = append(eventIDs, serviceEvent.EventID)
		if len(eventIDs) >= batch {
			if err = deleteServiceEvents(eventIDs); err != nil {
				return err
			}
			// 清空eventIDs
			eventIDs = eventIDs[:0]
		}
	}
	if len(eventIDs) > 0 {
		if err = deleteServiceEvents(eventIDs); err != nil {
			return err
		}
	}
	return nil
}

func (e *EventBus) Start(ctx context.Context) {
	run := func() {
		// 定时触发event_handler
		ticker := time.NewTicker(e.opt.RunInterval)
		for range ticker.C {
			if err := e.handleTransactions(); err != nil {
				e.logger.Error(err, "handle transaction failed")
			}

			if e.cb != nil {
				if err := e.handleEvents(); err != nil {
					if errors.Is(err, ErrServiceNotCreate) {
						_ = e.initService()
					}
					e.logger.Error(err, "handler events err")
				}
			}
		}
	}
	// 确保只启动一次
	e.once.Do(func() {
		// 添加定时清理任务
		if err := e.cleanCron.AddFunc(e.opt.CleanCron, func() {
			if err := e.cleanEvents(); err != nil {
				e.logger.Error(err, "clean events err")
			}
		}); err != nil {
			panic(err)
		}
		e.cleanCron.Start()
		go run()
	})
}

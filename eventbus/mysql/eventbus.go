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
	stdlog "log"
	"os"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/go-logr/logr"
	"github.com/go-logr/stdr"
	"github.com/robfig/cron"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/bytedance/dddfirework"
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

var defaultLogger = stdr.New(stdlog.New(os.Stderr, "", stdlog.LstdFlags|stdlog.Lshortfile)).WithName("mysql_eventbus")

type IRetryStrategy interface {
	// Next 获取下一次重试的策略，返回 nil 表示不再重试
	// 当 RetryInfo.RetryCount == 0 表示初始化状态，通过 Next 获取第一次重试信息
	Next(info *RetryInfo) *RetryInfo
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
		logger:        defaultLogger,
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

// Dispatch 框架模式下通过框架实现持久化，外部模式下手动存储
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
	return e.getDB(ctx).Create(pos).Error
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
	err := e.db.Where(ServicePO{Name: e.serviceName}).FirstOrCreate(service).Error
	if err != nil {
		return err
	}
	return nil
}

func (e *EventBus) lockService(tx *gorm.DB) (*ServicePO, error) {
	service := &ServicePO{}
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("name = ?", e.serviceName).
		First(service).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrServiceNotCreate
		}
		return nil, err
	}
	return service, nil
}

func (e *EventBus) getScanEvents(db *gorm.DB, service *ServicePO) ([]*EventPO, error) {
	if service.Offset == 0 {
		if e.opt.DefaultOffset != nil {
			service.Offset = *e.opt.DefaultOffset
		} else {
			lastEvent := &EventPO{}
			if err := db.Order("id").Last(lastEvent).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return nil, nil
				}
				return nil, err
			}
			// 不指定 offset 的情况下，默认从新的一条事件开始，且包含该条
			service.Offset = lastEvent.ID - 1
		}
	}
	eventPOs := make([]*EventPO, 0)
	query := db.Where("id > ?", service.Offset)
	if err := query.Order("id").Limit(e.opt.LimitPerRun).Find(&eventPOs).Error; err != nil {
		return nil, err
	}

	return eventPOs, nil
}

func (e *EventBus) getRetryEvents(db *gorm.DB, service *ServicePO) ([]*EventPO, error) {
	now := time.Now()
	retryIDs := make([]int64, 0)
	for _, info := range service.Retry {
		if info.RetryTime.Before(now) {
			retryIDs = append(retryIDs, info.ID)
		}
	}
	if len(retryIDs) == 0 {
		return nil, nil
	}

	eventPOs := make([]*EventPO, 0)
	if err := db.Where("id in ?", retryIDs).Find(&eventPOs).Error; err != nil {
		return nil, err
	}
	return eventPOs, nil
}

func (e *EventBus) doRetryStrategy(service *ServicePO, failedIDs []int64) (retry, failed []*RetryInfo) {
	// 没有定义重试策略，默认不重试直接失败
	if e.retryStrategy == nil {
		for _, id := range failedIDs {
			failed = append(failed, &RetryInfo{ID: id})
		}
		return
	}
	retryInfos := make(map[int64]*RetryInfo)
	for _, info := range service.Retry {
		retryInfos[info.ID] = info
	}

	for _, id := range failedIDs {
		info := retryInfos[id]
		if info == nil {
			info = &RetryInfo{ID: id}
		}

		newInfo := e.retryStrategy.Next(info)
		if newInfo != nil {
			retry = append(retry, newInfo)
		} else {
			failed = append(failed, info)
		}
	}
	return
}

func (e *EventBus) dispatchEvents(ctx context.Context, eventPOs []*EventPO) (success, failed []int64) {
	events := make(chan *EventPO, len(eventPOs))
	for _, e := range eventPOs {
		events <- e
	}
	close(events)

	wg := sync.WaitGroup{}
	for i := 0; i < e.opt.ConsumeConcurrent; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for po := range events {
				if err := e.cb(ctx, po.Event); err != nil {
					failed = append(failed, po.ID)
				} else {
					success = append(success, po.ID)
				}
			}
		}()
	}
	wg.Wait()
	return
}

func (e *EventBus) handleEvents() error {
	e.logger.Info("handle events")

	return e.db.Transaction(func(tx *gorm.DB) error {
		ctx := context.Background()
		service, err := e.lockService(tx)
		if err != nil {
			return err
		}
		scanEvents, err := e.getScanEvents(tx, service)
		if err != nil {
			return err
		}
		retryEvents, err := e.getRetryEvents(tx, service)
		if err != nil {
			return err
		}
		events := make([]*EventPO, 0)
		events = append(events, scanEvents...)
		events = append(events, retryEvents...)
		if len(events) == 0 {
			return nil
		}

		_, failedIDs := e.dispatchEvents(ctx, events)
		retry, failed := e.doRetryStrategy(service, failedIDs)
		service.Retry = retry
		service.Failed = append(service.Failed, failed...)
		if len(scanEvents) > 0 {
			last := scanEvents[len(scanEvents)-1]
			service.Offset = last.ID
		}
		return tx.Save(service).Error
	})
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
		if err := db.Clauses(clause.Locking{Strength: "UPDATE"}).Where(
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

func (e *EventBus) cleanEvents() error {
	e.logger.Info("clean events")
	// 查询failed 或重试中的event，不能清理
	// retry 重试的时候，重试成功，从retry 移除。重试次数超过设置，从retry 移除，加入failed
	// failed event不能删，这个相当于死信队列，留给用户做人工判断
	var services []*ServicePO
	err := e.db.Find(&services).Error
	if err != nil {
		return err
	}
	failedOrRetryingEventIDSet := map[int64]bool{}
	for _, service := range services {
		for _, si := range service.Retry {
			failedOrRetryingEventIDSet[si.ID] = true
		}
		for _, si := range service.Failed {
			failedOrRetryingEventIDSet[si.ID] = true
		}
	}

	// 查询消费最慢的service，其已经消费的event即为被所有service消费完的event。First 会自动加上主键排序所以使用Take
	var slowestServicePO ServicePO
	if err = e.db.Order("offset").Take(&slowestServicePO).Error; err != nil {
		return err
	}
	if slowestServicePO.Offset == 0 {
		// 最近消费时间没有赋值，可能还未触发消费，暂时不考虑清理
		return nil
	}
	lastEvent := &EventPO{}
	if err := e.db.Where("id = ?", slowestServicePO.Offset).First(lastEvent).Error; err != nil {
		return err
	}

	// 遍历可以清理的event，加上for update 防止多实例场景下并发操作问题
	query := e.db.Model(&EventPO{}).Clauses(clause.Locking{Strength: "UPDATE"})
	needCleanAt := time.Now().Add(-e.opt.RetentionTime)
	// 可能因为服务未启动等原因，event 很长时间都未被清理
	if lastEvent.CreatedAt.Before(needCleanAt) {
		needCleanAt = lastEvent.CreatedAt
	}
	// servicePO记录 EventConsumedID 的原因是：EventConsumedAt 只精确到毫秒，可能有多个事件event_created_at 一样。此时这些遗留的event等到下一个清理周期再被清理。
	query = query.Where("created_at < ?", needCleanAt)
	rows, err := query.Rows()
	if err != nil {
		return err
	}
	defer rows.Close()

	// 批量删除event
	deleteEvents := func(eventIDs []int64) error {
		if err := e.db.Where("id in ?", eventIDs).Delete(EventPO{}).Error; err != nil {
			return err
		}
		return nil
	}
	batch := 10
	eventIDs := make([]int64, 0)
	for rows.Next() {
		event := &EventPO{}
		err = e.db.ScanRows(rows, event)
		if err != nil {
			return err
		}
		if failedOrRetryingEventIDSet[event.ID] {
			continue
		}
		eventIDs = append(eventIDs, event.ID)
		if len(eventIDs) >= batch {
			if err = deleteEvents(eventIDs); err != nil {
				return err
			}
			// 清空eventIDs
			eventIDs = eventIDs[:0]
		}
	}
	if len(eventIDs) > 0 {
		if err = deleteEvents(eventIDs); err != nil {
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

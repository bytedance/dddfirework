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
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	"github.com/bytedance/dddfirework"
	exec_mysql "github.com/bytedance/dddfirework/executor/mysql"
	"github.com/bytedance/dddfirework/testsuit"
)

type testEntity struct {
	dddfirework.BaseEntity

	Name string
}

type testPO struct {
	ID   string
	Name string
}

func (o *testPO) GetID() string {
	return o.ID
}

func (o *testPO) TableName() string {
	return "test"
}

type testEvent struct {
	EType string
	Data  string
}

func (t *testEvent) GetType() dddfirework.EventType {
	return dddfirework.EventType(t.EType)
}

func (t *testEvent) GetSender() string {
	return t.Data
}

func initModel(db *gorm.DB) {
	if err := db.AutoMigrate(&EventPO{}, &ServicePO{}, &testPO{}, &Transaction{}, &ServiceEventPO{}); err != nil {
		panic(err)
	}

	exec_mysql.RegisterEntity2Model(&testEntity{}, func(entity, parent dddfirework.IEntity, op dddfirework.OpType) (exec_mysql.IModel, error) {
		e := entity.(*testEntity)
		return &testPO{
			ID:   e.GetID(),
			Name: e.Name,
		}, nil
	}, func(po exec_mysql.IModel, do dddfirework.IEntity) error {
		p, d := po.(*testPO), do.(*testEntity)
		d.SetID(p.ID)
		d.Name = p.Name
		return nil
	})
}

func init() {
	db := testsuit.InitMysql()
	initModel(db)
	db.Where("1 = 1").Delete(&EventPO{})
	db.Where("1 = 1").Delete(&ServicePO{})

	events := make([]*EventPO, 0)
	createTime := time.Now()
	e, _ := eventPersist(&dddfirework.DomainEvent{
		ID:        "0",
		Type:      "one",
		Payload:   []byte("{}"),
		CreatedAt: createTime,
	})
	events = append(events, e)

	for i := 1; i < 10; i++ {
		e, _ := eventPersist(&dddfirework.DomainEvent{
			ID:        fmt.Sprintf("%d", i),
			Type:      "test",
			Payload:   []byte("{}"),
			CreatedAt: createTime,
		})
		createTime = createTime.Add(time.Millisecond)
		events = append(events, e)
	}
	for i := 10; i < 20; i++ {
		e, _ := eventPersist(&dddfirework.DomainEvent{
			ID:        fmt.Sprintf("%d", i),
			Type:      "create_same_time",
			Payload:   []byte("{}"),
			CreatedAt: createTime,
		})
		events = append(events, e)
	}

	if err := db.Create(events).Error; err != nil {
		panic(err)
	}
}

func TestEventBusConcurrent(t *testing.T) {
	db := testsuit.InitMysql()

	mu := sync.Mutex{}
	ids := make(map[string]bool)
	events := make([]*dddfirework.DomainEvent, 0)

	eventBus := NewEventBus("test_concurrent", db, func(opt *Options) {
		opt.LimitPerRun = 5
		opt.ConsumeConcurrent = 1
		offset := int64(0)
		opt.DefaultOffset = &offset
	})
	for i := 1; i < 10; i++ {
		eventBus.Dispatch(context.Background(), &dddfirework.DomainEvent{
			ID:        fmt.Sprintf("TestEventBusConcurrent-%d", i),
			Type:      "TestEventBusConcurrent",
			Payload:   []byte("{}"),
			CreatedAt: time.Now(),
		})
	}
	eventBus.RegisterEventHandler(func(ctx context.Context, evt *dddfirework.DomainEvent) error {
		mu.Lock()
		defer mu.Unlock()
		if strings.HasPrefix(evt.ID, "TestEventBusConcurrent") {
			ids[evt.ID] = true
			events = append(events, evt)
		}
		return nil
	})

	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_ = eventBus.handleEvents()
			}
		}()
	}
	wg.Wait()

	var eventCount int64
	db.Model(&ServiceEventPO{}).Where("event_id LIKE ?", "TestEventBusConcurrent%").Count(&eventCount)
	// 保证所有事件都能消费到
	assert.Equal(t, eventCount, int64(len(ids)))
	curr := time.Time{}
	// 保证消费顺序一定是递增的
	for _, e := range events {
		assert.GreaterOrEqual(t, e.CreatedAt, curr)
		curr = e.CreatedAt
	}
}

func TestEventBusConcurrentFailed(t *testing.T) {
	ctx := context.Background()
	db := testsuit.InitMysql()

	eventBus := NewEventBus("test_concurrent_failed", db, func(opt *Options) {
		opt.RetryStrategy = &LimitRetry{
			Limit: -1,
		}
		opt.LimitPerRun = 200
		opt.ConsumeConcurrent = 10
	})
	eventBus.RegisterEventHandler(func(ctx context.Context, evt *dddfirework.DomainEvent) error {
		if evt.Type == "test_concurrent_failed" {
			return fmt.Errorf("failed")
		}
		return nil
	})

	for i := 0; i < 100; i++ {
		err := eventBus.Dispatch(ctx, dddfirework.NewDomainEvent(&testEvent{EType: "test_concurrent_failed", Data: "failed"}))
		assert.NoError(t, err)
	}

	err := eventBus.handleEvents()
	assert.NoError(t, err)

	var count int64
	err = db.Transaction(func(tx *gorm.DB) error {
		return tx.Model(&ServiceEventPO{}).Where("service = ?", "test_concurrent_failed").
			Where("status = ?", ServiceEventStatusFailed).Count(&count).Error
	})
	assert.NoError(t, err)
	assert.Equal(t, int(count), 100)

}

func TestEventBusRetry(t *testing.T) {
	db := testsuit.InitMysql()

	mu := sync.Mutex{}
	counts := map[string]int{}
	eventBus := NewEventBus("test_retry", db, func(opt *Options) {
		opt.LimitPerRun = 10
		opt.RetryStrategy = &LimitRetry{
			Limit: 5,
		}
		offset := int64(0)
		opt.DefaultOffset = &offset
	})
	eventBus.RegisterEventHandler(func(ctx context.Context, evt *dddfirework.DomainEvent) error {
		mu.Lock()
		counts[evt.ID] += 1
		mu.Unlock()

		if evt.ID == "0" {
			return fmt.Errorf("retry")
		}
		return nil
	})

	for i := 0; i < 30; i++ {
		if err := eventBus.handleEvents(); err != nil {
			assert.NoError(t, err)
		}
		time.Sleep(time.Millisecond * 10)
	}

	var eventCount int64
	db.Model(&ServiceEventPO{}).Count(&eventCount)
	assert.Equal(t, eventCount, int64(len(counts)))
	for id, count := range counts {
		if id == "0" {
			assert.Equal(t, 7, count)
		} else {
			assert.Equal(t, 1, count)
		}
	}
}

func TestEventBusRetryStrategy(t *testing.T) {
	ctx := context.Background()
	db := testsuit.InitMysql()

	mu := sync.Mutex{}
	counts := map[string]int{}
	eventBus := NewEventBus("test_retry_strategy", db, func(opt *Options) {
		opt.LimitPerRun = 100
		opt.ConsumeConcurrent = 10
		opt.RetryStrategy = &CustomRetry{
			Intervals: []time.Duration{
				10 * time.Millisecond,
				10 * time.Millisecond,
				1 * time.Hour,
			},
		}
		offset := int64(0)
		opt.DefaultOffset = &offset
	})
	eventBus.RegisterEventHandler(func(ctx context.Context, evt *dddfirework.DomainEvent) error {
		mu.Lock()
		counts[evt.ID] += 1
		mu.Unlock()

		if evt.ID == "0" {
			return fmt.Errorf("retry")
		}
		return nil
	})

	for i := 0; i < 10; i++ {
		if err := eventBus.Dispatch(ctx, dddfirework.NewDomainEvent(&testEvent{EType: "test_retry_strategy", Data: "retry"})); err != nil {
			assert.NoError(t, err)
		}
		if err := eventBus.handleEvents(); err != nil {
			assert.NoError(t, err)
		}
		time.Sleep(time.Millisecond * 10)
	}

	err := db.Transaction(func(tx *gorm.DB) error {
		var eventCount int64
		tx.Model(&EventPO{}).Count(&eventCount)
		assert.Equal(t, eventCount, int64(len(counts)))
		for id, count := range counts {
			if id == "0" {
				assert.Equal(t, 3, count)
			} else {
				assert.Equal(t, 1, count)
			}
		}

		var count int64
		err := tx.Model(&ServiceEventPO{}).Where("service = ?", "test_retry_strategy").
			Where("status = ?", ServiceEventStatusInit).Where("retry_count > 0").Count(&count).Error
		assert.NoError(t, err)
		assert.Equal(t, int(count), 1)
		return err
	})

	assert.NoError(t, err)
}

func TestEventBusFailed(t *testing.T) {
	ctx := context.Background()
	db := testsuit.InitMysql()

	eventBus := NewEventBus("test_fail", db, func(opt *Options) {
		opt.RetryStrategy = &LimitRetry{
			Limit: 2,
		}
		opt.ConsumeConcurrent = 1
	})
	eventBus.RegisterEventHandler(func(ctx context.Context, evt *dddfirework.DomainEvent) error {
		if evt.Type == "test_failed" {
			return fmt.Errorf("failed")
		}
		return nil
	})

	for i := 0; i < 10; i++ {
		err := eventBus.Dispatch(ctx, dddfirework.NewDomainEvent(&testEvent{EType: "test_failed", Data: "failed"}))
		assert.NoError(t, err)
	}

	for i := 0; i < 20; i++ {
		err := eventBus.handleEvents()
		assert.NoError(t, err)
	}

	var count int64
	err := db.Model(&ServiceEventPO{}).Where("service = ?", "test_fail").
		Where("status = ?", ServiceEventStatusFailed).Count(&count).Error
	assert.NoError(t, err)
	assert.Equal(t, int(count), 10)
}

func TestEngine(t *testing.T) {
	db := testsuit.InitMysql()

	ctx := context.Background()
	data := ""
	wg := sync.WaitGroup{}
	wg.Add(2)
	dddfirework.RegisterEventHandler("test_engine", func(ctx context.Context, evt *testEvent) error {
		data = evt.Data
		wg.Done()
		return nil
	})
	dddfirework.RegisterEventHandler("test_engine_tx", func(ctx context.Context, evt *testEvent) error {
		data = evt.Data
		wg.Done()
		return nil
	})

	eventBus := NewEventBus("test_engine", db)
	eventBus.Start(ctx)

	engine := dddfirework.NewEngine(nil, exec_mysql.NewExecutor(db), eventBus.Options()...)
	res := engine.NewStage().Main(func(ctx context.Context, repo *dddfirework.Repository) error {
		e := &testEntity{Name: "hello"}
		e.AddEvent(&testEvent{EType: "test_engine", Data: e.Name})
		e.AddEvent(&testEvent{EType: "test_engine_tx", Data: e.Name}, dddfirework.WithSendType(dddfirework.SendTypeTransaction))
		repo.Add(e)
		return nil
	}).Save(ctx)

	wg.Wait()
	assert.NoError(t, res.Error)
	assert.Equal(t, "hello", data)
}

type mockExecutor struct {
}

func (f *mockExecutor) Begin(ctx context.Context) (context.Context, error) { return ctx, nil }
func (f *mockExecutor) Commit(ctx context.Context) error                   { return fmt.Errorf("failed") }
func (f *mockExecutor) RollBack(ctx context.Context) error                 { return fmt.Errorf("failed") }

func (f *mockExecutor) Entity2Model(entity, parent dddfirework.IEntity, op dddfirework.OpType) (dddfirework.IModel, error) {
	return entity, nil
}

func (f *mockExecutor) Model2Entity(model dddfirework.IModel, entity dddfirework.IEntity) error {
	return nil
}

func (f *mockExecutor) Exec(ctx context.Context, action *dddfirework.Action) error { return nil }

func TestTXChecker(t *testing.T) {
	db := testsuit.InitMysql()

	ctx := context.Background()
	data := ""

	dddfirework.RegisterEventTXChecker("test_commit_failed", func(evt *testEvent) dddfirework.TXStatus {
		data = evt.Data
		return dddfirework.TXCommit
	})

	eventBus := NewEventBus("test_engine", db, func(opt *Options) {
		opt.TXCheckTimeout = time.Millisecond * 200
	})
	eventBus.Start(ctx)

	engine := dddfirework.NewEngine(nil, &mockExecutor{}, eventBus.Options()...)
	res := engine.NewStage().Main(func(ctx context.Context, repo *dddfirework.Repository) error {
		e := &testEntity{Name: "test_commit_failed"}
		e.AddEvent(&testEvent{EType: "test_commit_failed", Data: e.Name}, dddfirework.WithSendType(dddfirework.SendTypeTransaction))
		repo.Add(e)
		return nil
	}).Save(ctx)

	time.Sleep(time.Second * 1)

	assert.Error(t, res.Error)
	assert.Equal(t, "test_commit_failed", data)
}

func TestOuter(t *testing.T) {
	ctx := context.Background()
	db := testsuit.InitMysql()
	eventBus := NewEventBus("test_engine", db)
	eventBus.Start(ctx)

	var data string

	dddfirework.RegisterEventBus(eventBus)
	dddfirework.RegisterEventHandler("test_outer", func(ctx context.Context, evt *testEvent) error {
		data = evt.Data
		return nil
	})

	err := eventBus.Dispatch(ctx, dddfirework.NewDomainEvent(&testEvent{EType: "test_outer", Data: "gujuji"}))
	assert.NoError(t, err)

	err = eventBus.handleEvents()
	assert.NoError(t, err)
	assert.Equal(t, "gujuji", data)
}

func TestCommit(t *testing.T) {
	ctx := context.Background()
	db := testsuit.InitMysql()

	eventBus := NewEventBus("test_transaction", db)
	evt := dddfirework.NewDomainEvent(&testEvent{EType: "test_transaction", Data: "ttt"})
	ctx, err := eventBus.DispatchBegin(ctx, evt)
	assert.NoError(t, err)

	count := int64(0)
	err = db.Model(&EventPO{}).Where("id = ?", evt.ID).Count(&count).Error
	assert.NoError(t, err)
	assert.Equal(t, 0, int(count))

	err = eventBus.Commit(ctx)
	assert.NoError(t, err)

	err = db.Model(&EventPO{}).Where("event_id = ?", evt.ID).Count(&count).Error
	assert.NoError(t, err)
	assert.Equal(t, 1, int(count))
}

func TestCommitTimeout(t *testing.T) {
	ctx := context.Background()
	db := testsuit.InitMysql()

	eventBus := NewEventBus("test_transaction", db, func(opt *Options) {
		opt.TXCheckTimeout = time.Millisecond * 200
	})
	eventBus.RegisterEventTXChecker(func(evt *dddfirework.DomainEvent) dddfirework.TXStatus {
		return dddfirework.TXCommit
	})
	eventBus.Start(ctx)

	evt := dddfirework.NewDomainEvent(&testEvent{EType: "test_transaction", Data: "ttt"})
	_, err := eventBus.DispatchBegin(ctx, evt)
	assert.NoError(t, err)

	time.Sleep(time.Second)

	count := int64(0)
	err = db.Model(&EventPO{}).Where("event_id = ?", evt.ID).Count(&count).Error
	assert.NoError(t, err)
	assert.Equal(t, 1, int(count))
}

func TestRollback(t *testing.T) {
	ctx := context.Background()
	db := testsuit.InitMysql()

	eventBus := NewEventBus("test_transaction", db, func(opt *Options) {
		offset := int64(0)
		opt.DefaultOffset = &offset
	})
	evt := dddfirework.NewDomainEvent(&testEvent{EType: "test_transaction", Data: "ttt"})
	ctx, err := eventBus.DispatchBegin(ctx, evt)
	assert.NoError(t, err)

	err = eventBus.Rollback(ctx)
	assert.NoError(t, err)

	count := int64(0)
	err = db.Model(&EventPO{}).Where("id = ?", evt.ID).Count(&count).Error
	assert.NoError(t, err)
	assert.Equal(t, 0, int(count))
}

func TestClean(t *testing.T) {
	ctx := context.Background()
	// 使用sqlite防止数据被其它单测影响
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	assert.NoError(t, err)
	err = db.AutoMigrate(&ServiceEventPO{}, &EventPO{}, &ServicePO{}, &testPO{})
	assert.NoError(t, err)

	eventBus := NewEventBus("test_clean", db, func(opt *Options) {
		// 消费完成的事件保留时间为0
		opt.RetentionTime = 0 * time.Hour
		opt.ConsumeConcurrent = 1
	})
	// 插入一些事件
	num := 10
	for num > 0 {
		evt := dddfirework.NewDomainEvent(&testEvent{EType: "test_clean", Data: "ttt"})
		err = eventBus.Dispatch(ctx, evt)
		// 必须停1ms，因为mysql datetime 只精确到1ms，1ms以内的event无法立即清理
		time.Sleep(1 * time.Millisecond)
		assert.NoError(t, err)
		num--
	}

	eventBus.RegisterEventHandler(func(ctx context.Context, evt *dddfirework.DomainEvent) error {
		t.Logf("handle event ID %s,CreatedAt %s", evt.ID, evt.CreatedAt)
		return nil
	})
	// 处理事件
	err = eventBus.handleEvents()
	assert.NoError(t, err)
	// 清理事件
	err = eventBus.cleanEvents()
	assert.NoError(t, err)

	events := []*EventPO{}
	err = db.Model(&EventPO{}).Find(&events).Error
	assert.NoError(t, err)
	eventIDs := make([]int64, 0)
	for _, event := range events {
		eventIDs = append(eventIDs, event.ID)
	}
	// 确认现存的event的已经没有成功的了
	count := int64(0)
	err = db.Model(&ServiceEventPO{}).Where("event_id in ?", eventIDs).Where("status = ?", ServiceEventStatusSuccess).Count(&count).Error
	assert.NoError(t, err)
	assert.Equal(t, 0, int(count))
}

// 测试failed 或 retry 事件未被删除
func TestCleanFailed(t *testing.T) {
	ctx := context.Background()
	// 使用sqlite防止数据被其它单测影响
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	assert.NoError(t, err)
	err = db.AutoMigrate(&EventPO{}, &ServicePO{}, &testPO{}, &ServiceEventPO{})
	assert.NoError(t, err)

	eventBus := NewEventBus("test_clean", db, func(opt *Options) {
		// 消费完成的事件保留时间为0
		opt.RetentionTime = 0 * time.Hour
	})
	// 插入一些事件
	num := 10
	for num > 0 {
		evt := dddfirework.NewDomainEvent(&testEvent{EType: "test_clean", Data: "ttt"})
		err = eventBus.Dispatch(ctx, evt)
		// 必须停1ms，因为mysql datetime 只精确到1ms，1ms以内的event无法立即清理
		time.Sleep(1 * time.Millisecond)
		assert.NoError(t, err)
		num--
	}

	eventBus.RegisterEventHandler(func(ctx context.Context, evt *dddfirework.DomainEvent) error {
		t.Logf("handle event ID %s,CreatedAt %s", evt.ID, evt.CreatedAt)
		if rand.Intn(2) > 0 {
			return fmt.Errorf("test clean error")
		}
		return nil
	})
	// 处理事件
	err = eventBus.handleEvents()
	assert.NoError(t, err)
	// 清理事件
	err = eventBus.cleanEvents()
	assert.NoError(t, err)

	// 校验除了 failed 和 retry中的event 其它的都删掉了
	events := []*EventPO{}
	err = db.Model(&EventPO{}).Find(&events).Error
	assert.NoError(t, err)
	eventIDs := make([]int64, 0)
	for _, event := range events {
		eventIDs = append(eventIDs, event.ID)
	}
	// 确认现存的event的已经没有成功的了
	count := int64(0)
	err = db.Model(&ServiceEventPO{}).Where("event_id in ?", eventIDs).Where("status = ?", ServiceEventStatusSuccess).Count(&count).Error
	assert.NoError(t, err)
	assert.Equal(t, 0, int(count))
}

func TestCleanConcurrent(t *testing.T) {
	ctx := context.Background()
	// sqlite在并发场景下部分特性与mysql不同
	// 使用独立database防止数据被其它单测影响
	db := testsuit.InitMysql()
	db = testsuit.InitMysqlWithDatabase(db, "test_clean")
	if err := db.AutoMigrate(&EventPO{}, &ServicePO{}, &testPO{}, &ServiceEventPO{}); err != nil {
		panic(err)
	}
	eventBus := NewEventBus("test_clean", db, func(opt *Options) {
		// 消费完成的事件保留时间为0
		opt.RetentionTime = 0 * time.Hour
	})
	// 插入一些事件
	num := 30
	for num > 0 {
		evt := dddfirework.NewDomainEvent(&testEvent{EType: "test_clean", Data: "ttt"})
		err := eventBus.Dispatch(ctx, evt)
		// 必须停1ms，因为mysql datetime 只精确到1ms，1ms以内的event无法立即清理
		time.Sleep(1 * time.Millisecond)
		assert.NoError(t, err)
		num--
	}

	eventBus.RegisterEventHandler(func(ctx context.Context, evt *dddfirework.DomainEvent) error {
		t.Logf("handle event ID %s,CreatedAt %s", evt.ID, evt.CreatedAt)
		return nil
	})
	// 处理事件
	err := eventBus.handleEvents()
	assert.NoError(t, err)
	// 并发清理事件
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := eventBus.cleanEvents()
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	// 校验除了 failed 和 retry中的event 其它的都删掉了
	events := []*EventPO{}
	err = db.Model(&EventPO{}).Find(&events).Error
	assert.NoError(t, err)
	eventIDs := make([]int64, 0)
	for _, event := range events {
		eventIDs = append(eventIDs, event.ID)
	}
	// 确认现存的event的已经没有成功的了
	count := int64(0)
	err = db.Model(&ServiceEventPO{}).Where("event_id in ?", eventIDs).Where("status = ?", ServiceEventStatusSuccess).Count(&count).Error
	assert.NoError(t, err)
	assert.Equal(t, 0, int(count))
}

func TestFIFO(t *testing.T) {
	ctx := context.Background()
	db := testsuit.InitMysql()
	if err := db.AutoMigrate(&EventPO{}, &ServicePO{}, &testPO{}, &ServiceEventPO{}); err != nil {
		panic(err)
	}
	eventBus := NewEventBus("test_fifo", db, func(opt *Options) {
		opt.ConsumeConcurrent = 5
	})
	// 插入一些事件
	num := 5
	for num > 0 {
		evt := dddfirework.NewDomainEvent(&testEvent{EType: "test_fifo", Data: "ttt"}, dddfirework.WithSendType(dddfirework.SendTypeFIFO))
		err := eventBus.Dispatch(ctx, evt)
		time.Sleep(1 * time.Millisecond)
		assert.NoError(t, err)
		num--
	}

	events := make([]*dddfirework.DomainEvent, 0)
	eventBus.RegisterEventHandler(func(ctx context.Context, evt *dddfirework.DomainEvent) error {
		t.Logf("handle event ID %s,CreatedAt %s", evt.ID, evt.CreatedAt)
		// 必须停1ms，因为mysql datetime 只精确到1ms，这样每个event 的run_at 有明显的大小差别
		time.Sleep(1 * time.Millisecond)
		if evt.Type == "test_fifo" {
			events = append(events, evt)
		}
		return nil
	})
	// 处理事件
	for i := 0; i < 10; i++ {
		// 得等一下， 不然 scanEvents 时根据next_time < time.Now 可能小于service_event插入时的next_time
		time.Sleep(100 * time.Millisecond)
		err := eventBus.handleEvents()
		assert.NoError(t, err)
	}

	// 保证消费顺序一定是递增的
	curr := time.Time{}
	for _, e := range events {
		assert.GreaterOrEqual(t, e.CreatedAt, curr)
		curr = e.CreatedAt
	}

}

func TestLaxFIFO(t *testing.T) {
	// 初始化随机数种子
	rand.Seed(time.Now().UnixNano())
	ctx := context.Background()
	db := testsuit.InitMysql()
	if err := db.AutoMigrate(&EventPO{}, &ServicePO{}, &testPO{}, &ServiceEventPO{}); err != nil {
		panic(err)
	}
	eventBus := NewEventBus("test_lax_fifo", db, func(opt *Options) {
		opt.ConsumeConcurrent = 2
		opt.RetryLimit = -1
		opt.RetryInterval = 1 * time.Millisecond
	})
	// 插入一些事件
	num := 5
	for num > 0 {
		evt := dddfirework.NewDomainEvent(&testEvent{EType: "test_lax_fifo", Data: "ttt"}, dddfirework.WithSendType(dddfirework.SendTypeLaxFIFO))
		err := eventBus.Dispatch(ctx, evt)
		time.Sleep(1 * time.Millisecond)
		assert.NoError(t, err)
		num--
	}
	events := make([]*dddfirework.DomainEvent, 0)
	eventBus.RegisterEventHandler(func(ctx context.Context, evt *dddfirework.DomainEvent) error {
		t.Logf("handle event ID %s,CreatedAt %s", evt.ID, evt.CreatedAt)
		// 必须停1ms，因为mysql datetime 只精确到1ms，这样每个event 的run_at 有明显的大小差别
		time.Sleep(1 * time.Millisecond)
		if evt.Type == "test_lax_fifo" {
			events = append(events, evt)
		}
		randomNum := rand.Intn(10-1+1) + 1
		if randomNum%2 == 0 {
			return fmt.Errorf("random error %d", randomNum)
		}
		return nil
	})
	// 处理事件
	for i := 0; i < 20; i++ {
		// 得等一下， 不然 scanEvents 时根据next_time < time.Now 可能小于service_event插入时的next_time
		time.Sleep(100 * time.Millisecond)
		err := eventBus.handleEvents()
		assert.NoError(t, err)
	}

	// 保证消费顺序一定是递增的
	curr := time.Time{}
	for _, e := range events {
		assert.GreaterOrEqual(t, e.CreatedAt, curr)
		curr = e.CreatedAt
	}

}

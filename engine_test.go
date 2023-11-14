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
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/bytedance/dddfirework/testsuit"
	"github.com/stretchr/testify/assert"
)

type supplier struct {
	BaseEntity

	Name string
}

type product struct {
	BaseEntity

	Name      string
	Suppliers []*supplier
	Index     int
}

type coupon struct {
	BaseEntity

	Name string
}

type order struct {
	BaseEntity

	Title    string
	Products []*product
	Coupon   []*coupon
}

type TestCommand struct {
	Command

	id       string
	deleteID string

	order  *order
	Result string
}

func (c *TestCommand) Init(ctx context.Context) (lockKeys []string, err error) {
	return []string{c.id, c.deleteID}, nil
}

// Build 构建并返回聚合根实体
func (c *TestCommand) Build(ctx context.Context, builder DomainBuilder) (roots []IEntity, err error) {
	order1 := &order{
		BaseEntity: NewBase(c.id),
		Products:   []*product{},
	}
	if err := builder.Build(ctx, order1, &order1.Products); err != nil {
		return nil, err
	}
	sort.Slice(order1.Products, func(i, j int) bool {
		return order1.Products[i].Index < order1.Products[j].Index
	})
	order2 := &order{
		BaseEntity: NewBase(c.deleteID),
	}
	if err := builder.Build(ctx, order2, &order1.Products); err != nil {
		return nil, err
	}
	c.order = order1
	return []IEntity{order1, order2}, nil
}

// Act 调用实体行为
func (c *TestCommand) Act(ctx context.Context, container RootContainer, roots ...IEntity) error {
	testOrder, testDeleteOrder := roots[0].(*order), roots[1].(*order)
	testOrder.Title = "order1 update"
	testOrder.Dirty()

	// 新增子实体
	testOrder.Coupon = append(testOrder.Coupon, &coupon{
		BaseEntity: NewBase("coupon1"),
		Name:       "coupon",
	})
	// 新增子实体的子实体
	testOrder.Products[0].Suppliers = append(testOrder.Products[0].Suppliers, &supplier{
		BaseEntity: NewBase("supplier1"),
		Name:       "supplier",
	})

	// 修改子实体
	testOrder.Products[0].Name = "product update"
	testOrder.Products[0].Dirty()

	// 删除子实体
	testOrder.Products = testOrder.Products[:1]

	container.Remove(testDeleteOrder)
	return nil
}

func (c *TestCommand) PostSave(ctx context.Context, res *Result) {
	c.Result = c.order.GetID()
}

func TestEngine(t *testing.T) {
	ctx := context.Background()
	db := TestDB{
		Data: map[string]*testModel{},
	}

	testOrder := &order{
		Title: "order1",
		Products: []*product{{
			BaseEntity: NewBase("product1"),
			Name:       "product1",
			Suppliers:  []*supplier{},
			Index:      1,
		}, {
			BaseEntity: NewBase("product2"),
			Name:       "product2",
			Index:      2,
		}},
		Coupon: nil,
	}

	testDeleteOrder := &order{
		Title: "order2",
	}
	locker := testsuit.NewMemLock()
	executor := &MapExecutor{DB: &db}
	engine := NewEngine(locker, executor)
	res := engine.Create(ctx, testOrder, testDeleteOrder)
	assert.NoError(t, res.Error)

	assert.Contains(t, db.Data, testOrder.GetID())
	assert.Contains(t, db.Data, testDeleteOrder.GetID())

	cmd := &TestCommand{id: testOrder.GetID(), deleteID: testDeleteOrder.GetID()}
	res = engine.RunCommand(ctx, cmd)
	assert.NoError(t, res.Error)
	assert.Equal(t, testOrder.GetID(), cmd.Result)

	orderPO := db.Data[testOrder.GetID()]
	assert.Equal(t, "order1 update", orderPO.Name)

	// 测试新增子实体
	assert.Contains(t, db.Data, "coupon1")
	assert.Contains(t, db.Data, "supplier1")

	// 测试更新实体
	productPO := db.Data["product1"]
	assert.Equal(t, productPO.ParentID, testOrder.GetID())
	assert.Equal(t, "product update", productPO.Name)

	// 测试删除根实体、子实体
	assert.NotContains(t, db.Data, testDeleteOrder.GetID())
	assert.NotContains(t, db.Data, "product2")
}

func TestRootContainer(t *testing.T) {
	c := &EntityContainer{}

	testOrders := []*order{
		{
			BaseEntity: NewBase("test"),
			Title:      "test",
		},
		{
			BaseEntity: NewBase("test2"),
			Title:      "test",
		},
	}
	assert.NoError(t, c.Add(testOrders[0]))
	assert.NoError(t, c.Add(testOrders[1]))

	children := c.GetChildren()
	assert.Len(t, children["meta"], 2)
	assert.Equal(t, children["meta"][0], testOrders[0])

	assert.NoError(t, c.Remove(testOrders[0]))
	children = c.GetChildren()
	assert.Len(t, children["meta"], 1)
	assert.Equal(t, children["meta"][0], testOrders[1])
}

func TestBuildError(t *testing.T) {
	ctx := context.Background()
	res := NewEngine(nil, nil, WithoutTransaction).NewStage().Main(func(ctx context.Context, repo *Repository) error {
		return fmt.Errorf("test")
	}).Save(ctx)
	assert.Error(t, res.Error)
}

func TestRecursiveDelete(t *testing.T) {
	ctx := context.Background()
	db := TestDB{
		Data: map[string]*testModel{},
	}

	testOrder := &order{
		Title: "order1",
		Products: []*product{{
			BaseEntity: NewBase("product1"),
			Name:       "product1",
			Suppliers: []*supplier{{
				BaseEntity: NewBase("supplier1"),
				Name:       "supplier1",
			}, {
				BaseEntity: NewBase("supplier2"),
				Name:       "supplier2",
			}},
			Index: 1,
		}},
	}
	locker := testsuit.NewMemLock()
	executor := &MapExecutor{DB: &db}
	NewEngine(locker, executor).Create(ctx, testOrder)

	res := NewEngine(locker, executor, WithRecursiveDelete).Delete(ctx, testOrder)
	assert.NoError(t, res.Error)

	assert.NotContains(t, db.Data, testOrder.GetID())
	assert.NotContains(t, db.Data, "product1")
	assert.NotContains(t, db.Data, "supplier1")
	assert.NotContains(t, db.Data, "supplier2")
}

func TestEntityMove(t *testing.T) {
	ctx := context.Background()
	db := TestDB{
		Data: map[string]*testModel{},
	}

	p := &product{
		BaseEntity: NewBase("product"),
		Name:       "product",
	}
	testOrder1 := &order{
		Title:    "order1",
		Products: []*product{p},
	}

	testOrder2 := &order{
		Title: "order2",
	}
	engine := NewEngine(testsuit.NewMemLock(), &MapExecutor{DB: &db})
	engine.Create(ctx, testOrder1, testOrder2)

	res := engine.NewStage().Main(func(ctx context.Context, repo *Repository) error {
		if err := repo.CustomGet(ctx, func(ctx context.Context, root ...IEntity) {}, testOrder1, testOrder2); err != nil {
			return err
		}
		testOrder1.Products = nil
		testOrder2.Products = []*product{p}
		return nil
	}).Save(ctx)

	assert.NoError(t, res.Error)
	assert.Len(t, res.Actions, 1)
	assert.Equal(t, OpUpdate, res.Actions[0].Op)
}

type testHookEntity struct {
	BaseEntity

	Name             string
	BeforeCreateFlag int
	AfterCreateFlag  int
	BeforeUpdateFlag int
	AfterUpdateFlag  int
	BeforeDeleteFlag int
	AfterDeleteFlag  int
}

func (t *testHookEntity) BeforeCreate(ctx context.Context) error {
	t.BeforeCreateFlag++
	return nil
}

func (t *testHookEntity) AfterCreate(ctx context.Context) error {
	t.AfterCreateFlag++
	return nil
}

func (t *testHookEntity) BeforeUpdate(ctx context.Context) error {
	t.BeforeUpdateFlag++
	return nil
}

func (t *testHookEntity) AfterUpdate(ctx context.Context) error {
	t.AfterUpdateFlag++
	return nil
}

func (t *testHookEntity) BeforeDelete(ctx context.Context) error {
	t.BeforeDeleteFlag++
	return nil
}

func (t *testHookEntity) AfterDelete(ctx context.Context) error {
	t.AfterDeleteFlag++
	return nil
}

type testEntityHookCmd struct {
	Command

	entity *testHookEntity
}

func (c *testEntityHookCmd) Act(ctx context.Context, container RootContainer, roots ...IEntity) (err error) {
	container.Add(c.entity)
	_ = c.Commit(ctx)

	c.entity.Name = "update"
	c.entity.Dirty()
	_ = c.Commit(ctx)

	container.Remove(c.entity)
	return
}

func TestEntityHook(t *testing.T) {
	ctx := context.Background()

	entity := &testHookEntity{Name: "test"}
	res := NewEngine(nil, &emptyExecutor{}).RunCommand(ctx, &testEntityHookCmd{entity: entity})
	assert.NoError(t, res.Error)
	assert.Equal(t, 1, entity.BeforeCreateFlag)
	assert.Equal(t, 1, entity.AfterCreateFlag)
	assert.Equal(t, 1, entity.BeforeUpdateFlag)
	assert.Equal(t, 1, entity.AfterUpdateFlag)
	assert.Equal(t, 1, entity.BeforeDeleteFlag)
	assert.Equal(t, 1, entity.AfterDeleteFlag)
}

type testEventHandler struct {
	Command

	evt *testEvent
}

func (c *testEventHandler) Act(ctx context.Context, container RootContainer, roots ...IEntity) error {
	container.Add(&order{BaseEntity: NewBase(c.evt.Data)})
	return nil
}

func TestEventHandler(t *testing.T) {
	ctx := context.Background()
	db := TestDB{
		Data: map[string]*testModel{},
	}
	engine := NewEngine(nil, &MapExecutor{DB: &db})
	engine.RegisterEventHandler("test", func(evt *testEvent) *testEventHandler {
		return &testEventHandler{evt: evt}
	})
	engine.RegisterEventHandler("test", func(evt *DomainEvent) *testEventHandler {
		return &testEventHandler{evt: &testEvent{"testdomainevent"}}
	})
	err := onEvent(ctx, NewDomainEvent(&testEvent{Data: "testorder"}))

	assert.NoError(t, err)
	assert.Contains(t, db.Data, "testorder")
	assert.Contains(t, db.Data, "testdomainevent")
}

func TestEventRegisterFailedHandler(t *testing.T) {
	db := TestDB{
		Data: map[string]*testModel{},
	}
	engine := NewEngine(nil, &MapExecutor{DB: &db})

	func() {
		defer func() {
			assert.NotEmpty(t, recover())
		}()
		engine.RegisterEventHandler("test", "test")
	}()

	func() {
		defer func() {
			assert.NotEmpty(t, recover())
		}()
		engine.RegisterEventHandler("test", func(evt *DomainEvent) (*testEventHandler, error) { return nil, nil })
	}()

	func() {
		defer func() {
			assert.NotEmpty(t, recover())
		}()
		engine.RegisterEventHandler("test", func(evt DomainEvent) *testEventHandler { return nil })
	}()

	func() {
		defer func() {
			assert.NotEmpty(t, recover())
		}()
		engine.RegisterEventHandler("test", func(evt DomainEvent) error { return nil })
	}()

}

type emptyEventBus struct {
}

// Dispatch 发送领域事件到 IEventBus，这是个同步阻塞调用
func (emptyEventBus) Dispatch(ctx context.Context, evt ...*DomainEvent) error {
	return nil
}

// RegisterEventHandler 注册事件回调，IEventBus 的实现必须保证收到事件同步调用该回调
func (emptyEventBus) RegisterEventHandler(cb DomainEventHandler) {}

func TestEventPersist(t *testing.T) {
	ctx := context.Background()
	db := TestDB{
		Data: map[string]*testModel{},
	}
	var testOrder *order
	res := NewEngine(testsuit.NewMemLock(), &MapExecutor{DB: &db}, WithEventBus(&emptyEventBus{}),
		WithEventPersist(func(event *DomainEvent) (IModel, error) {
			return &testModel{
				ID:   event.ID,
				Name: string(event.Type),
			}, nil
		})).Run(ctx, func(ctx context.Context, repo *Repository) error {
		testOrder = &order{
			Title: "order1",
		}
		testOrder.AddEvent(&testEvent{Data: "hello"})
		repo.Add(testOrder)
		return nil
	})
	assert.NoError(t, res.Error)
	assert.NotEmpty(t, testOrder.GetEvents())
	assert.Contains(t, db.Data, testOrder.GetEvents()[0].ID)
}

func BenchmarkCreate100Orders(b *testing.B) {
	testOrders := make([]IEntity, 0)
	for i := 0; i < 100; i++ {
		testOrders = append(testOrders, &order{
			Title: fmt.Sprintf("order-%d", i),
		})
	}

	for j := 0; j < b.N; j++ {
		db := TestDB{
			Data: map[string]*testModel{},
		}
		NewEngine(nil, &MapExecutor{DB: &db}).Create(context.Background(), testOrders...)
	}
}

func BenchmarkUpdateOrders(b *testing.B) {
	for j := 0; j < b.N; j++ {
		db := TestDB{
			Data: map[string]*testModel{
				"order1": {
					ID:         "order1",
					Name:       "order1",
					CreateTime: time.Now(),
					UpdateTime: time.Now(),
				},
				"product1": {
					ID:         "product1",
					Name:       "product1",
					CreateTime: time.Now(),
					UpdateTime: time.Now(),
				}},
		}

		testOrder := &order{
			BaseEntity: NewBase("order1"),
			Title:      "order1",
			Products: []*product{{
				BaseEntity: NewBase("product1"),
				Name:       "product1",
			}},
		}
		title := fmt.Sprintf("update %d", j)
		newID := fmt.Sprintf("new%d", j)
		res := NewEngine(nil, &MapExecutor{DB: &db}).Run(context.Background(), func(ctx context.Context, repo *Repository) error {
			if err := repo.CustomGet(ctx, func(ctx context.Context, root ...IEntity) {}, testOrder); err != nil {
				return err
			}
			testOrder.Title = title
			testOrder.Dirty()

			testOrder.Products = []*product{{
				BaseEntity: NewBase(newID),
				Name:       fmt.Sprintf("product%d", j),
			}}
			return nil
		})
		if res.Error != nil {
			panic(res.Error)
		}
	}
}

type testModel struct {
	ID         string
	Name       string
	ParentID   string
	Entity     IEntity
	Index      int
	CreateTime time.Time
	UpdateTime time.Time
}

func (t *testModel) GetID() string {
	return t.ID
}

type TestDB struct {
	Data map[string]*testModel
}

type MapExecutor struct {
	DB *TestDB
}

func (f *MapExecutor) Begin(ctx context.Context) (context.Context, error) { return ctx, nil }
func (f *MapExecutor) Commit(ctx context.Context) error                   { return nil }
func (f *MapExecutor) RollBack(ctx context.Context) error                 { return nil }

func (f *MapExecutor) Entity2Model(entity, parent IEntity, op OpType) (IModel, error) {
	parentID := ""
	if parent != nil {
		parentID = parent.GetID()
	}
	po := &testModel{
		ID:       entity.GetID(),
		ParentID: parentID,
		Entity:   entity,
	}
	switch v := entity.(type) {
	case *order:
		po.Name = v.Title
	case *product:
		po.Name = v.Name
		po.Index = v.Index
	}

	return po, nil
}

func (f *MapExecutor) Model2Entity(model IModel, entity IEntity) error {
	po := model.(*testModel)

	switch e := entity.(type) {
	case *order:
		e.SetID(po.ID)
		e.Title = po.Name
	case *product:
		e.SetID(po.ID)
		e.Name = po.Name
		e.Index = po.Index
	}
	return nil
}

func (f *MapExecutor) Exec(ctx context.Context, action *Action) error {
	if action.Op == OpQuery {
		query := action.Query.(*testModel)
		result := action.QueryResult.(*[]*testModel)
		if query.ID != "" {
			m := f.DB.Data[query.ID]
			if m == nil {
				return nil
			}
			*result = append(*result, m)
		} else if query.ParentID != "" {
			for _, item := range f.DB.Data {
				if item.ParentID == query.ParentID {
					*result = append(*result, item)
				}
			}
		}
		return nil
	} else {
		for _, m := range action.Models {
			tm := m.(*testModel)
			switch action.Op {
			case OpDelete:
				delete(f.DB.Data, tm.ID)
			case OpInsert:
				tm.CreateTime = time.Now()
				tm.UpdateTime = time.Now()
				f.DB.Data[tm.ID] = tm
			case OpUpdate:
				tm.UpdateTime = time.Now()
				f.DB.Data[tm.ID] = tm
			}

		}
	}

	return nil
}

func TestEntityContainer(t *testing.T) {
	container := EntityContainer{}
	for i := 0; i < 100; i++ {
		_ = container.Add(&order{BaseEntity: NewBase(fmt.Sprintf("%d", i))})
	}

	assert.Equal(t, 100, len(container.roots))

	del := func(roots ...IEntity) {
		for _, r := range roots {
			_ = container.Remove(r)
		}
	}
	roots := make([]IEntity, len(container.roots))
	copy(roots, container.roots)
	del(roots...)

	assert.Equal(t, 0, len(container.roots))
	assert.Equal(t, 100, len(container.deleted))
}

type emptyExecutor struct {
}

func (f *emptyExecutor) Begin(ctx context.Context) (context.Context, error) { return ctx, nil }
func (f *emptyExecutor) Commit(ctx context.Context) error                   { return nil }
func (f *emptyExecutor) RollBack(ctx context.Context) error                 { return nil }
func (f *emptyExecutor) Entity2Model(entity, parent IEntity, op OpType) (IModel, error) {
	return entity, nil
}
func (f *emptyExecutor) Model2Entity(model IModel, entity IEntity) error { return nil }
func (f *emptyExecutor) Exec(ctx context.Context, actions *Action) error { return nil }

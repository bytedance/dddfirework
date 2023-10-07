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
	"os"
	"testing"

	ddd "github.com/bytedance/dddfirework"
	"github.com/bytedance/dddfirework/testsuit"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

type user struct {
	ddd.BaseEntity

	Name string
}

type userPO struct {
	ID      string `gorm:"primaryKey;column:id"`
	OrderID string `gorm:"column:order_id"`
	Name    string `gorm:"column:name"`
}

func (o *userPO) GetID() string {
	return o.ID
}

func (o *userPO) TableName() string {
	return "user"
}

type supplier struct {
	ddd.BaseEntity

	Name string
}

type product struct {
	ddd.BaseEntity

	Name      string
	Tags      []string
	Suppliers []*supplier
}

type order struct {
	ddd.BaseEntity

	ID       string
	Title    string
	User     *user
	Products []*product
}

func (o *order) SetID(id string) {
	o.ID = id
}

func (o *order) GetID() string {
	return o.ID
}

type orderPO struct {
	ID    string `gorm:"primaryKey;column:id"`
	Title string `gorm:"column:title"`
}

func (o *orderPO) GetID() string {
	return o.ID
}

func (o *orderPO) TableName() string {
	return "order"
}

type productPO struct {
	ID      string   `gorm:"primaryKey;column:id"`
	OrderID string   `gorm:"column:order_id"`
	Name    string   `gorm:"column:name"`
	Tags    []string `gorm:"type:json;default:(-);serializer:json;comment:'标签'"`
}

func (o *productPO) GetID() string {
	return o.ID
}

func (o *productPO) TableName() string {
	return "product"
}

type supplierPO struct {
	ID        string `gorm:"primaryKey"`
	ProductID string `gorm:"column:product_id"`
	Name      string `gorm:"column:name"`
}

func (o *supplierPO) GetID() string {
	return o.ID
}

func (o *supplierPO) TableName() string {
	return "supplier"
}

func init() {
	db := testsuit.InitMysql()
	initModel(db)
	if os.Getenv("LOCAL_BENCHMARK") == "true" {
		initBenchmark(db)
	}
}

func initModel(db *gorm.DB) {
	if err := db.AutoMigrate(&orderPO{}, &productPO{}, &supplierPO{}, &userPO{}); err != nil {
		panic(err)
	}

	db.Where("1=1").Delete(&orderPO{})
	db.Where("1=1").Delete(&productPO{})
	db.Where("1=1").Delete(&supplierPO{})
	db.Where("1=1").Delete(&userPO{})

	RegisterEntity2Model(&order{}, func(entity, parent ddd.IEntity, op ddd.OpType) (IModel, error) {
		order := entity.(*order)
		return &orderPO{
			ID:    order.GetID(),
			Title: order.Title,
		}, nil
	}, func(po IModel, do ddd.IEntity) error {
		s, t := po.(*orderPO), do.(*order)
		t.SetID(s.ID)
		t.Title = s.Title
		return nil
	})

	RegisterEntity2Model(&product{}, func(entity, parent ddd.IEntity, op ddd.OpType) (IModel, error) {
		do := entity.(*product)
		return &productPO{
			ID:      do.GetID(),
			OrderID: parent.GetID(),
			Name:    do.Name,
			Tags:    do.Tags,
		}, nil
	}, func(po IModel, do ddd.IEntity) error {
		s, t := po.(*productPO), do.(*product)
		t.SetID(s.ID)
		t.Name = s.Name
		t.Tags = s.Tags
		return nil
	})

	RegisterEntity2Model(&supplier{}, func(entity, parent ddd.IEntity, op ddd.OpType) (IModel, error) {
		do := entity.(*supplier)
		return &supplierPO{
			ID:   do.GetID(),
			Name: do.Name,
		}, nil
	}, func(po IModel, do ddd.IEntity) error {
		fr, to := po.(*supplierPO), do.(*supplier)
		to.SetID(fr.ID)
		to.Name = fr.Name
		return nil
	})

	RegisterEntity2Model(&user{}, func(entity, parent ddd.IEntity, op ddd.OpType) (IModel, error) {
		do := entity.(*user)
		return &userPO{
			ID:      do.GetID(),
			OrderID: parent.GetID(),
			Name:    do.Name,
		}, nil

	}, func(po IModel, do ddd.IEntity) error {
		fr, to := po.(*userPO), do.(*user)
		to.SetID(fr.ID)
		to.Name = fr.Name
		return nil
	})
}

type Case struct {
	ddd.Command

	TestOrderID  string
	TestDeleteID string

	db *gorm.DB
}

// Init 会在锁和事务之前执行，可选返回锁ID
func (c *Case) Init(ctx context.Context) (lockIDs []string, err error) {
	return []string{c.TestOrderID}, nil
}

// Build 构建实体
func (c *Case) Build(ctx context.Context, builder ddd.DomainBuilder) (roots []ddd.IEntity, err error) {
	testOrder := &order{
		ID:       c.TestOrderID,
		User:     &user{},
		Products: []*product{},
	}
	if err := builder.Build(ctx, testOrder, testOrder.User, &testOrder.Products); err != nil {
		return nil, err
	}

	testDeleteOrder := &order{ID: c.TestDeleteID}
	if err := builder.Build(ctx, testDeleteOrder); err != nil {
		return nil, err
	}

	return []ddd.IEntity{testOrder, testDeleteOrder}, nil
}

// Act 调用实体行为
func (c *Case) Act(ctx context.Context, container ddd.RootContainer, roots ...ddd.IEntity) error {
	testOrder, testDeleteOrder := roots[0].(*order), roots[1].(*order)

	// 删除子实体
	testOrder.Products = testOrder.Products[:1]

	// 新增子实体
	testOrder.Products = append(testOrder.Products, &product{
		BaseEntity: ddd.NewBase("product_new"),
		Name:       "product_new",
	})
	// 新增子实体的子实体
	testOrder.Products[0].Suppliers = append(testOrder.Products[0].Suppliers, &supplier{
		BaseEntity: ddd.NewBase("supplier1"),
		Name:       "supplier",
	})

	// 修改子实体
	testOrder.Products[0].Name = "product update"
	testOrder.Products[0].Tags = []string{"tag1", "tag2"}
	testOrder.Products[0].Dirty()

	container.Remove(testDeleteOrder)
	return nil
}

func TestExecutor(t *testing.T) {
	ctx := context.Background()
	db := testsuit.InitMysql()

	testOrder := &order{
		ID:    "order1",
		Title: "order111",
		User: &user{
			BaseEntity: ddd.NewBase("user1"),
			Name:       "peter",
		},
		Products: []*product{{
			BaseEntity: ddd.NewBase("product1"),
			Name:       "product",
			Suppliers:  []*supplier{},
		}, {
			BaseEntity: ddd.NewBase("product2"),
			Name:       "product2",
			Suppliers:  []*supplier{},
		}},
	}

	testDeleteOrder := &order{
		ID:    "order2",
		Title: "order22",
	}
	engine := ddd.NewEngine(testsuit.NewMemLock(), NewExecutor(db))
	res := engine.Create(ctx, testOrder, testDeleteOrder)
	assert.NoError(t, res.Error)

	assert.NoError(t, db.First(&orderPO{}, "id = ?", "order1").Error)
	assert.NoError(t, db.First(&productPO{}, "id = ?", "product1").Error)
	assert.NoError(t, db.First(&productPO{}, "id = ?", "product2").Error)
	assert.NoError(t, db.First(&orderPO{}, "id = ?", "order2").Error)

	res = engine.RunCommand(ctx, &Case{
		db:           db,
		TestOrderID:  "order1",
		TestDeleteID: "order2",
	})

	assert.NoError(t, res.Error)

	// 测试新增子实体
	assert.NoError(t, db.First(&productPO{}, "id = ?", "product_new").Error)
	assert.NoError(t, db.First(&supplierPO{}, "id = ?", "supplier1").Error)

	// 测试更新实体
	p := productPO{}
	assert.NoError(t, db.Model(&productPO{}).First(&p, "id = ?", "product1").Error)
	assert.Equal(t, testOrder.GetID(), p.OrderID)
	assert.Equal(t, "product update", p.Name)
	assert.Len(t, p.Tags, 2)

	// 测试删除根实体、子实体
	assert.ErrorIs(t, db.First(&orderPO{}, "id = ?", "order2").Error, gorm.ErrRecordNotFound)
	assert.ErrorIs(t, db.First(&productPO{}, "id = ?", "product2").Error, gorm.ErrRecordNotFound)
}

func TestDelete(t *testing.T) {
	db := testsuit.InitMysql()

	o := &order{
		Title: "testdelete",
	}
	ctx := context.Background()

	engine := ddd.NewEngine(testsuit.NewMemLock(), NewExecutor(db))
	res := engine.Create(ctx, o)
	assert.NoError(t, res.Error)
	assert.NoError(t, db.First(&orderPO{}, "id = ?", o.ID).Error)

	res = engine.Delete(ctx, o)
	assert.NoError(t, res.Error)

	assert.ErrorIs(t, db.First(&orderPO{}, "id = ?", o.ID).Error, gorm.ErrRecordNotFound)
}

func TestRollback(t *testing.T) {
	db := testsuit.InitMysql()

	ctx := context.Background()

	engine := ddd.NewEngine(testsuit.NewMemLock(), NewExecutor(db))
	res := engine.NewStage().Act(func(ctx context.Context, container ddd.RootContainer, roots ...ddd.IEntity) error {
		container.Add(&order{
			ID:    "testrollback",
			Title: "testrollback",
		})
		container.Add(&order{
			ID:    "testrollback",
			Title: "testrollback",
		})
		return nil
	}).Save(ctx)
	assert.NotNil(t, res.Error)
	assert.ErrorIs(t, db.First(&orderPO{}, "id = ?", "testrollback").Error, gorm.ErrRecordNotFound)
}

func initBenchmark(db *gorm.DB) {
	testOrder := &order{
		ID:    "order1",
		Title: "order111",
		User: &user{
			Name: "peter",
		},
		Products: []*product{{
			Name:      "product",
			Suppliers: []*supplier{},
		}},
	}
	if res := ddd.NewEngine(nil, NewExecutor(db)).Create(context.Background(), testOrder); res.Error != nil {
		panic(res.Error)
	}
}

var insertTable = []struct {
	input int
}{
	{input: 1},
	{input: 10},
	{input: 100},
}

func BenchmarkInsert(b *testing.B) {
	ctx := context.Background()
	db := testsuit.InitMysql()

	engine := ddd.NewEngine(nil, NewExecutor(db))
	for _, v := range insertTable {
		b.Run(fmt.Sprintf("insert_size_%d", v.input), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				orders := make([]ddd.IEntity, 0)
				for i := 0; i < v.input; i++ {
					orders = append(orders, &order{Title: fmt.Sprintf("test%d", i)})
				}
				if res := engine.Create(ctx, orders...); res.Error != nil {
					panic(res.Error)
				}
			}
		})
	}
}

func BenchmarkUpdate(b *testing.B) {
	ctx := context.Background()
	db := testsuit.InitMysql()

	engine := ddd.NewEngine(nil, NewExecutor(db))
	for i := 0; i < b.N; i++ {
		if res := engine.NewStage().Build(func(ctx context.Context, builder ddd.DomainBuilder) (roots []ddd.IEntity, err error) {
			testOrder := &order{
				ID:       "order1",
				User:     &user{},
				Products: []*product{},
			}
			if err := builder.Build(ctx, testOrder, testOrder.User, &testOrder.Products); err != nil {
				return nil, err
			}
			return []ddd.IEntity{testOrder}, nil
		}).Act(func(ctx context.Context, container ddd.RootContainer, roots ...ddd.IEntity) error {
			testOrder := roots[0].(*order)
			testOrder.Title = fmt.Sprintf("order updated %d", rand.Int63())
			testOrder.Dirty()

			testOrder.Products = []*product{{
				Name: fmt.Sprintf("new product %d", rand.Int63()),
			}}
			return nil
		}).Save(ctx); res.Error != nil {
			panic(res.Error)
		} else if len(res.Actions) != 3 {
			panic("actions wrong")
		}
	}
}

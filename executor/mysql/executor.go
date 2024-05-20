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
	"reflect"
	"time"

	ddd "github.com/bytedance/dddfirework"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

var ErrInvalidDB = fmt.Errorf("invalid db")
var ErrNoTransaction = fmt.Errorf("no transaction")

type IModel interface {
	GetID() string
	TableName() string
}

type Entity2Model func(entity, parent ddd.IEntity, op ddd.OpType) (IModel, error)
type Model2Entity func(po IModel, do ddd.IEntity) error

type Converter struct {
	entity2Model Entity2Model
	model2Entity Model2Entity
}

var entity2ModelRegistry = map[reflect.Type]Converter{}

func realType(v interface{}) reflect.Type {
	t := reflect.TypeOf(v)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t
}

func RegisterEntity2Model(entity ddd.IEntity, f1 Entity2Model, f2 Model2Entity) {
	entity2ModelRegistry[realType(entity)] = Converter{
		entity2Model: f1,
		model2Entity: f2,
	}
}

type IConverter interface {
	Entity2Model(entity, parent ddd.IEntity, op ddd.OpType) (IModel, error)
	Model2Entity(po IModel, do ddd.IEntity) error
}

// RegisterConverter 使用时直接 mysql.RegisterConverter(&do{}, converter)
func RegisterConverter(entity ddd.IEntity, converter IConverter) {
	entity2ModelRegistry[realType(entity)] = Converter{
		entity2Model: converter.Entity2Model,
		model2Entity: converter.Model2Entity,
	}
}

type execFunc func(db *gorm.DB, a *ddd.Action) error

var opMap = map[ddd.OpType]execFunc{
	ddd.OpInsert: func(db *gorm.DB, a *ddd.Action) error {
		po := a.Models[0]
		poType := reflect.ValueOf(po).Type()
		pos := reflect.MakeSlice(reflect.SliceOf(poType), 0, 0)
		for _, a := range a.Models {
			pos = reflect.Append(pos, reflect.ValueOf(a))
		}
		return db.Create(pos.Interface()).Error
	},
	ddd.OpUpdate: func(db *gorm.DB, a *ddd.Action) error {
		for i, m := range a.Models {
			if len(a.PrevModels) > i {
				fields := DiffModel(m, a.PrevModels[i])
				if err := db.Select(fields).Updates(m).Error; err != nil {
					return err
				}
			} else {
				if err := db.Save(m.(IModel)).Error; err != nil {
					return err
				}
			}
		}
		return nil
	},
	ddd.OpDelete: func(db *gorm.DB, a *ddd.Action) error {
		po := a.Models[0]
		s, err := schema.Parse(po, schemaCache, db.NamingStrategy)
		if err != nil {
			return err
		}

		if len(s.PrimaryFields) == 1 {
			// 单主键支持批量删除
			pk := s.PrimaryFields[0].DBName
			poType := reflect.Indirect(reflect.ValueOf(po)).Type()
			newPO := reflect.New(poType).Interface()
			if len(a.Models) == 1 {
				return db.Where(pk+" = ?", a.Models[0].GetID()).Delete(newPO).Error
			}
			ids := make([]string, 0)
			for _, m := range a.Models {
				ids = append(ids, m.GetID())
			}
			return db.Where(pk+" in ?", ids).Delete(newPO).Error
		}
		// 复合主键的一个个删
		for _, m := range a.Models {
			if err := db.Delete(m).Error; err != nil {
				return err
			}
		}
		return nil
	},
	ddd.OpQuery: func(db *gorm.DB, a *ddd.Action) error {
		res := db.Where(a.Query).Find(a.QueryResult)
		return res.Error
	},
}

// 确保外面拿不到内部的 key
type contextKey string

type Executor struct {
	db    *gorm.DB
	txKey contextKey
}

func NewExecutor(db *gorm.DB) *Executor {
	return &Executor{
		db:    db,
		txKey: contextKey(fmt.Sprintf("executor_tx_%d", time.Now().Unix())),
	}
}

func (e *Executor) Begin(ctx context.Context) (context.Context, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if e.db == nil {
		return ctx, ErrInvalidDB
	}

	tx := e.db.Begin()
	if tx.Error != nil {
		return ctx, fmt.Errorf("start transation failed, err=%s", tx.Error)
	}
	return context.WithValue(ctx, e.txKey, tx), nil
}

func (e *Executor) Commit(ctx context.Context) error {
	if e.db == nil {
		return ErrInvalidDB
	}
	val := ctx.Value(e.txKey)
	if val == nil {
		return ErrNoTransaction
	}

	tx := val.(*gorm.DB)
	return tx.Commit().Error
}

func (e *Executor) RollBack(ctx context.Context) error {
	if e.db == nil {
		return ErrInvalidDB
	}
	val := ctx.Value(e.txKey)
	if val == nil {
		return ErrNoTransaction
	}

	tx := val.(*gorm.DB)
	return tx.Rollback().Error
}

func (e *Executor) Entity2Model(entity, parent ddd.IEntity, op ddd.OpType) (ddd.IModel, error) {
	if converter, ok := entity2ModelRegistry[realType(entity)]; !ok {
		return nil, ddd.ErrEntityNotRegister
	} else {
		return converter.entity2Model(entity, parent, op)
	}
}

func (e *Executor) Model2Entity(model ddd.IModel, entity ddd.IEntity) error {
	if converter, ok := entity2ModelRegistry[realType(entity)]; !ok {
		return ddd.ErrEntityNotRegister
	} else {
		po, ok := model.(IModel)
		if !ok {
			return fmt.Errorf("model must implement IModel")
		}
		return converter.model2Entity(po, entity)
	}
}

func (e *Executor) Diff(ctx context.Context, curr, prev ddd.IModel) ddd.IModel {
	return curr
}

func (e *Executor) Exec(ctx context.Context, action *ddd.Action) error {
	db := e.db

	val := ctx.Value(e.txKey)
	if val != nil {
		db = val.(*gorm.DB)
	}

	f, ok := opMap[action.Op]
	if !ok {
		return fmt.Errorf("unsupport op(%d) for mysql executor", action.Op)
	}
	return f(db, action)
}

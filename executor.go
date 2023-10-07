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
)

type OpType int8

const (
	OpUnknown OpType = 0
	OpInsert  OpType = 1
	OpUpdate  OpType = 2
	OpDelete  OpType = 3
	OpQuery   OpType = 4
)

var ErrEntityNotRegister = fmt.Errorf("entity not registered")

type IModel interface {
	GetID() string
}

type Action struct {
	Op OpType

	Models      []IModel    // 当前待操作模型，stage 确保一个 Action 下都是同类的模型
	PrevModels  []IModel    // 基于快照生成的模型，跟 Models 一一对应，Executor 需要对两者做差异比对后更新
	Query       IModel      // 指定查询字段的数据模型
	QueryResult interface{} // Model 的 slice 的指针，形如 *[]ExampleModel
}

type ActionResult struct {
	Data interface{}
}

type ITransaction interface {
	// Begin 开启事务，返回带有事务标识的 context，该 context 会原样传递给 Commit 或者 RollBack 方法
	Begin(ctx context.Context) (context.Context, error)
	// Commit 提交事务
	Commit(ctx context.Context) error
	// RollBack 回滚事务
	RollBack(ctx context.Context) error
}

type IConverter interface {
	// Entity2Model 实体转化为数据模型，entity: 当前实体 parent: 父实体 op: 调用场景，实体新建、实体更新、实体删除、实体查询等
	// 注意，对于Model上entity没有对应的字段，除新建场景外，其他需要保持为数据类型的零值，新建场景可以额外指定默认值
	// 实体未注册返回 ErrEntityNotRegister 错误
	Entity2Model(entity, parent IEntity, op OpType) (IModel, error)
	// Model2Entity 模型转换为实体，在创建实体的时候调用
	Model2Entity(model IModel, entity IEntity) error
}

type IExecutor interface {
	ITransaction
	IConverter

	Exec(ctx context.Context, actions *Action) error
}

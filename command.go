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

var cmdType = reflect.TypeOf((*ICommand)(nil)).Elem()

// Deprecated: 请直接用 ICommandMain + ICommandInit(可选) + ICommandPostSave(可选) 或者 MainFunc 的方式定义命令
type ICommand interface {
	// Init 会在锁和事务之前执行，可进行数据校验，前置准备工作，可选返回锁ID
	Init(ctx context.Context) (lockKeys []string, err error)

	// Build 构建并返回聚合根实体，框架会为返回值自动生成快照，作为持久化比对的依据
	// 注意，Create 的情况不需要在 Build 方法返回
	Build(ctx context.Context, builder DomainBuilder) (roots []IEntity, err error)

	// Act 调用实体行为
	Act(ctx context.Context, container RootContainer, roots ...IEntity) (err error)

	// PostSave Save 事务完成后回调，可以执行组装返回数据等操作
	PostSave(ctx context.Context, res *Result)
}

// ICommandMain Command 的业务逻辑，对应 Build + Act 方法
type ICommandMain interface {
	Main(ctx context.Context, repo Repository) (err error)
}

// ICommandInit Command 的初始化方法，会在锁和事务之前执行，可进行数据校验，前置准备工作
// 可选返回 lockKeys，框架会用 lockKeys 自动加锁
type ICommandInit interface {
	Init(ctx context.Context) (lockKeys []string, err error)
}

// ICommandPostSave Command 事务完成后回调，可以执行组装返回数据等操作
type ICommandPostSave interface {
	PostSave(ctx context.Context, res *Result)
}

type IStageSetter interface {
	SetStage(s StageAgent)
}

type StageAgent struct {
	st *Stage
}

func (s *StageAgent) Commit(ctx context.Context) error {
	return s.st.commit(ctx)
}

func (s *StageAgent) Output(data interface{}) {
	s.st.setOutput(data)
}

// Deprecated: 请直接用 ICommandMain 或者 MainFunc 的方式定义命令
type Command struct {
	stage StageAgent
}

func (c *Command) Init(ctx context.Context) (lockIDs []string, err error) {
	return nil, nil
}

func (c *Command) Build(ctx context.Context, builder DomainBuilder) (roots []IEntity, err error) {
	return nil, nil
}

func (c *Command) Act(ctx context.Context, container RootContainer, roots ...IEntity) (err error) {
	return
}

func (c *Command) PostSave(ctx context.Context, res *Result) {
}

// Commit 提交当前事务，对当前的变更执行持久化操作，新建的实体会获得 ID
func (c *Command) Commit(ctx context.Context) error {
	return c.stage.Commit(ctx)
}

// Output 设定命令的返回值，data 会被赋值到 Result.Output
// 该方法可以在 Init - Build - Act 中调用，调用后，后续的方法将不会被执行
// 例如在 Init 中调用 Return，Build、Act 方法不会执行，但是 PostSave 会被执行
func (c *Command) Output(data interface{}) {
	c.stage.Output(data)
}

func (c *Command) SetStage(s StageAgent) {
	c.stage = s
}

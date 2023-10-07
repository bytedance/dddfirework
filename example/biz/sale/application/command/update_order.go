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

package command

import (
	"context"

	"github.com/bytedance/dddfirework"
	"github.com/bytedance/dddfirework/example/biz/sale/domain"
	"github.com/bytedance/dddfirework/example/biz/sale/infrastructure/repo"
)

type UpdateOrderOpt struct {
	Remark *string
}

type UpdateOrderCommand struct {
	dddfirework.Command

	orderID string
	opt     UpdateOrderOpt
}

func NewUpdateOrderCommand(orderID string, opt UpdateOrderOpt) *UpdateOrderCommand {
	return &UpdateOrderCommand{
		orderID: orderID,
		opt:     opt,
	}
}

func (c *UpdateOrderCommand) Init(ctx context.Context) (lockIDs []string, err error) {
	return []string{c.orderID}, nil
}

func (c *UpdateOrderCommand) Build(ctx context.Context, builder dddfirework.DomainBuilder) (roots []dddfirework.IEntity, err error) {
	order, err := repo.GetOrder(ctx, builder, c.orderID)
	if err != nil {
		return nil, err
	}
	return []dddfirework.IEntity{order}, nil
}

func (c *UpdateOrderCommand) Act(ctx context.Context, container dddfirework.RootContainer, roots ...dddfirework.IEntity) error {
	order := roots[0].(*domain.Order)
	order.Update(domain.UpdateOrderOpt{Remark: c.opt.Remark})
	return nil
}

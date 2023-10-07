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
	"github.com/bytedance/dddfirework/example/common/dto/sale"
)

type CreateOrderResult struct {
	OrderID string
}

type CreateOrderCommand struct {
	dddfirework.Command

	userID  string
	items   []*sale.SaleItem
	coupons []*sale.Coupon

	Result *CreateOrderResult
}

func NewCreateOrderCommand(userID string, items []*sale.SaleItem, coupons []*sale.Coupon) *CreateOrderCommand {
	return &CreateOrderCommand{
		userID:  userID,
		items:   items,
		coupons: coupons,
	}
}

func (c *CreateOrderCommand) Act(ctx context.Context, container dddfirework.RootContainer, roots ...dddfirework.IEntity) error {
	order, err := domain.NewOrder(c.userID, c.items, c.coupons)
	if err != nil {
		return err
	}

	container.Add(order)

	//Commit 操作为可选项，目的是为了即刻获得待新建的订单 ID，构造返回值
	if err := c.Commit(ctx); err != nil {
		return err
	}
	c.Output(order)
	return nil
}

func (c *CreateOrderCommand) PostSave(ctx context.Context, res *dddfirework.Result) {
	if res.Error == nil {
		c.Result = &CreateOrderResult{
			OrderID: res.Output.(*domain.Order).ID,
		}
	}
}

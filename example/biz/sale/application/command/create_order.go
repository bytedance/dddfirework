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

func (c *CreateOrderCommand) Main(ctx context.Context, repo *dddfirework.Repository) error {
	order, err := domain.NewOrder(c.userID, c.items, c.coupons)
	if err != nil {
		return err
	}

	repo.Add(order)

	// 持久化后为新实体自动设置 ID
	if err := repo.Save(ctx); err != nil {
		return err
	}
	c.Result = &CreateOrderResult{OrderID: order.ID}
	return nil
}

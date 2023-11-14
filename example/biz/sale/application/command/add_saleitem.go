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

type AddSaleItemCommand struct {
	orderID string
	item    *sale.SaleItem
}

func NewAddSaleItemCommand(orderID string, item *sale.SaleItem) *AddSaleItemCommand {
	return &AddSaleItemCommand{orderID: orderID, item: item}
}

func (c *AddSaleItemCommand) Init(ctx context.Context) ([]string, error) {
	return []string{c.orderID}, nil
}

func (c *AddSaleItemCommand) Main(ctx context.Context, repo *dddfirework.Repository) error {
	order := &domain.Order{ID: c.orderID}
	if err := repo.Get(ctx, order); err != nil {
		return err
	}
	order.AddSaleItem(c.item.Code, c.item.Name, c.item.Price, c.item.Count)
	return nil
}

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

	ddd "github.com/bytedance/dddfirework"
	"github.com/bytedance/dddfirework/example/biz/sale/domain"
	"github.com/bytedance/dddfirework/example/common/dto/sale"
)

type AddCouponCommand struct {
	orderID string
	coupon  *sale.Coupon
}

func NewAddCouponCommand(orderID string, coupon *sale.Coupon) *AddCouponCommand {
	return &AddCouponCommand{
		orderID: orderID,
		coupon:  coupon,
	}
}

func (c *AddCouponCommand) Init(ctx context.Context) ([]string, error) {
	return []string{c.orderID}, nil
}

func (c *AddCouponCommand) Main(ctx context.Context, repo *ddd.Repository) error {
	order := &domain.Order{ID: c.orderID}
	if err := repo.Get(ctx, order); err != nil {
		return err
	}
	if err := order.AddCoupon(c.coupon.ID, c.coupon.Rule, c.coupon.Discount); err != nil {
		return err
	}
	return nil
}

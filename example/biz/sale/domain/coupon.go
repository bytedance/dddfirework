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

package domain

import (
	"context"

	ddd "github.com/bytedance/dddfirework"
	sale_event "github.com/bytedance/dddfirework/example/common/domain_event/sale"
)

type Coupon struct {
	ddd.BaseEntity

	CouponID string // 券ID
	Rule     string // 规则描述
	Discount int64  // 折扣数

	order *Order
}

func (c *Coupon) AfterCreate(ctx context.Context) error {
	c.AddEvent(sale_event.NewOrderCouponAddedEvent(c.order.ID, c.CouponID))
	return nil
}

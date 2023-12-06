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
	"fmt"

	ddd "github.com/bytedance/dddfirework"
	sale_event "github.com/bytedance/dddfirework/example/common/domain_event/sale"
	"github.com/bytedance/dddfirework/example/common/dto/sale"
)

type RuleEngine interface {
	Validate(order *Order, rule string) error
}

var ruler OrderRuleEngine

type OrderRuleEngine struct {
}

func (o *OrderRuleEngine) Validate(order *Order, rule string) error {
	// validate here
	return nil
}

type UpdateOrderOpt struct {
	Remark *string
}

type Order struct {
	ddd.BaseEntity

	ID          string
	UserID      string
	TotalAmount int64
	Remark      string
	Items       []*SaleItem
	Coupons     []*Coupon
}

func NewOrder(userID string, items []*sale.SaleItem, coupons []*sale.Coupon) (*Order, error) {
	order := &Order{
		UserID: userID,
	}
	for _, item := range items {
		order.AddSaleItem(item.Code, item.Name, item.Price, item.Count)
	}

	for _, coupon := range coupons {
		if err := order.AddCoupon(coupon.ID, coupon.Rule, coupon.Discount); err != nil {
			return nil, err
		}
	}
	return order, nil
}

func (o *Order) SetID(id string) {
	o.ID = id
}

func (o *Order) GetID() string {
	return o.ID
}

func (o *Order) AfterCreate(ctx context.Context) error {
	o.AddEvent(sale_event.NewOrderCreatedEvent(o.GetID()))
	return nil
}

func (o *Order) AfterDelete(ctx context.Context) error {
	o.AddEvent(sale_event.NewOrderDeletedEvent(o.GetID()))
	return nil
}

func (o *Order) Update(opt UpdateOrderOpt) {
	if opt.Remark != nil {
		o.Remark = *opt.Remark
	}
	// dirty 标记实体是否需要 update，应该在实体行为内部调用，不需要应用层明确感知
	o.Dirty()
}

// 校验优惠券规则，删除无效优惠券
func (o *Order) fixCoupons() (invalid []*Coupon) {
	valid := make([]*Coupon, 0)
	for _, c := range o.Coupons {
		if err := ruler.Validate(o, c.Rule); err != nil {
			invalid = append(invalid, c)
		} else {
			valid = append(valid, c)
		}
	}
	o.Coupons = valid
	return invalid
}

// 计算订单价格，保证一致
func (o *Order) calPrice() {
	total := int64(0)
	for _, item := range o.Items {
		total += item.Price * int64(item.Count)
	}
	for _, c := range o.Coupons {
		total -= c.Discount
	}
	if o.TotalAmount != total {
		o.TotalAmount = total
		// 订单属性有变更，需要标记 Dirty
		o.Dirty()
	}
}

func (o *Order) AddSaleItem(code, name string, price int64, count int32) {
	o.Items = append(o.Items, &SaleItem{
		Code:  code,
		Name:  name,
		Price: price,
		Count: count,
		order: o,
	})
	o.fixCoupons()
	o.calPrice()
}

func (o *Order) DeleteSaleItem(id string) {
	newItems := make([]*SaleItem, 0)
	for _, item := range o.Items {
		if item.GetID() != id {
			newItems = append(newItems, item)
		}
	}

	o.Items = newItems
	o.fixCoupons()
	o.calPrice()
}

func (o *Order) AddCoupon(couponID, rule string, discount int64) error {
	o.Coupons = append(o.Coupons, &Coupon{
		CouponID: couponID,
		Rule:     rule,
		Discount: discount,

		order: o,
	})
	if invalid := o.fixCoupons(); len(invalid) > 0 {
		return fmt.Errorf("coupon invalid")
	}
	o.calPrice()
	return nil
}

func (o *Order) DeleteCoupon(id string) error {
	newCoupons := make([]*Coupon, 0)
	for _, item := range o.Coupons {
		if item.GetID() != id {
			newCoupons = append(newCoupons, item)
		}
	}
	o.Coupons = newCoupons

	if invalid := o.fixCoupons(); len(invalid) > 0 {
		return fmt.Errorf("coupon invalid")
	}
	o.calPrice()
	return nil
}

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

package sale

import (
	ddd "github.com/bytedance/dddfirework"
)

const EventOrderCreated ddd.EventType = "order_created"
const EventOrderDeleted ddd.EventType = "order_deleted"
const EventOrderSaleItemAdded ddd.EventType = "order_sale_item_added"

// OrderCreatedEvent 事件定义，建议以Event结尾，过去式命名
type OrderCreatedEvent struct {
	OrderID string
}

func NewOrderCreatedEvent(orderID string) *OrderCreatedEvent {
	return &OrderCreatedEvent{OrderID: orderID}
}

func (e OrderCreatedEvent) GetType() ddd.EventType {
	return EventOrderCreated
}

func (e OrderCreatedEvent) GetSender() string {
	return e.OrderID
}

type OrderDeletedEvent struct {
	OrderID string
}

func NewOrderDeletedEvent(orderID string) *OrderDeletedEvent {
	return &OrderDeletedEvent{OrderID: orderID}
}

func (e OrderDeletedEvent) GetType() ddd.EventType {
	return EventOrderDeleted
}

func (e OrderDeletedEvent) GetSender() string {
	return e.OrderID
}

type OrderSaleItemAddedEvent struct {
	OrderID    string
	SaleItemID string
}

func NewOrderSaleItemAddedEvent(orderID, itemID string) *OrderSaleItemAddedEvent {
	return &OrderSaleItemAddedEvent{
		OrderID:    orderID,
		SaleItemID: itemID,
	}
}

func (e OrderSaleItemAddedEvent) GetType() ddd.EventType {
	return EventOrderSaleItemAdded
}

func (e OrderSaleItemAddedEvent) GetSender() string {
	return e.OrderID
}

type OrderCouponAddedEvent struct {
	OrderID  string
	CouponID string
}

func NewOrderCouponAddedEvent(orderID, couponID string) *OrderCouponAddedEvent {
	return &OrderCouponAddedEvent{
		OrderID:  orderID,
		CouponID: couponID,
	}
}

func (e OrderCouponAddedEvent) GetType() ddd.EventType {
	return EventOrderSaleItemAdded
}

func (e OrderCouponAddedEvent) GetSender() string {
	return e.OrderID
}

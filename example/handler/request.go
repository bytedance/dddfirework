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

package handler

import (
	"github.com/bytedance/dddfirework/example/common/dto/sale"
)

type CreateOrderRequest struct {
	User    string           `thrift:"User,1,required" frugal:"1,required,string" json:"User"`
	Items   []*sale.SaleItem `thrift:"Items,2,required" frugal:"2,required,list<SaleItem>" json:"Items"`
	Coupons []*sale.Coupon   `thrift:"Coupons,3,optional" frugal:"3,optional,list<Coupon>" json:"Coupons,omitempty"`
}

type CreateOrderResponse struct {
	OrderID string `thrift:"OrderID,1,required" frugal:"1,required,string" json:"OrderID"`
}

type UpdateOrderRequest struct {
	ID     string  `thrift:"ID,1,required" frugal:"1,required,string" json:"ID"`
	Remark *string `thrift:"Remark,2,optional" frugal:"2,optional,string" json:"Remark,omitempty"`
}

type UpdateOrderResponse struct {
}

type DeleteOrderRequest struct {
	ID string `thrift:"ID,1,required" frugal:"1,required,string" json:"ID"`
}

type DeleteOrderResponse struct {
}

type GetOrderRequest struct {
	ID string `thrift:"ID,1,required" frugal:"1,required,string" json:"ID"`
}

type GetOrderResponse struct {
	Order *sale.Order `thrift:"Order,1" frugal:"1,default,Order" json:"Order"`
}

type GetOrderListRequest struct {
	UserID          *string `thrift:"UserID,1,optional" frugal:"1,optional,string" json:"UserID,omitempty"`
	CreateTimeBegin *int64  `thrift:"CreateTimeBegin,2,optional" frugal:"2,optional,i64" json:"CreateTimeBegin,omitempty"`
	CreateTimeEnd   *int64  `thrift:"CreateTimeEnd,3,optional" frugal:"3,optional,i64" json:"CreateTimeEnd,omitempty"`
	Offset          *int32  `thrift:"Offset,4,optional" frugal:"4,optional,i32" json:"Offset,omitempty"`
	Limit           *int32  `thrift:"Limit,5,optional" frugal:"5,optional,i32" json:"Limit,omitempty"`
}

type GetOrderListResponse struct {
	Items []*sale.SimpleOrder `thrift:"Items,1" frugal:"1,default,list<SimpleOrder>" json:"Items"`
}

type AddSaleItemRequest struct {
	OrderID string         `thrift:"OrderID,1,required" frugal:"1,required,string" json:"OrderID"`
	Item    *sale.SaleItem `thrift:"Item,2,required" frugal:"2,required,SaleItem" json:"Item"`
}

type AddSaleItemResponse struct {
}

type AddCouponRequest struct {
	OrderID string       `thrift:"OrderID,1,required" frugal:"1,required,string" json:"OrderID"`
	Coupon  *sale.Coupon `thrift:"Coupon,2,required" frugal:"2,required,Coupon" json:"Coupon"`
}

type AddCouponResponse struct {
}

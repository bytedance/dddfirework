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

package query

import (
	"context"

	"github.com/bytedance/dddfirework/example/biz/sale/application/query/pack"
	"github.com/bytedance/dddfirework/example/biz/sale/infrastructure/dal"
	"github.com/bytedance/dddfirework/example/common/dto/sale"
	"gorm.io/gorm"
)

type FindOpt struct {
	ID       *int64
	Category *string
	Search   *string
}

var saleDAL *dal.DAL

func Init(db *gorm.DB) {
	saleDAL = dal.NewDAL(db)
}

func GetOrder(ctx context.Context, id string) (*sale.Order, error) {
	order, err := saleDAL.GetOrderByID(ctx, id)
	if err != nil {
		return nil, err
	}
	items, err := saleDAL.GetOrderSaleItems(ctx, id)
	if err != nil {
		return nil, err
	}
	coupons, err := saleDAL.GetOrderCoupons(ctx, id)
	if err != nil {
		return nil, err
	}
	return &sale.Order{
		ID:          order.GetID(),
		UserID:      order.User,
		TotalAmount: order.TotalAmount,
		Items:       pack.MakeSaleItemList(items),
		Coupons:     pack.MakeCouponList(coupons),
	}, nil

}

func GetOrderList(ctx context.Context, opt dal.SearchOrderOpt) ([]*sale.SimpleOrder, error) {
	orders, err := saleDAL.GetOrderList(ctx, opt)
	if err != nil {
		return nil, err
	}
	return pack.MakeSimpleOrderList(orders), nil
}

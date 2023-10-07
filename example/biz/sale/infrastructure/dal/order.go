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

package dal

import (
	"context"
	"time"

	"gorm.io/gorm"

	"github.com/bytedance/dddfirework/example/biz/sale/infrastructure/po"
)

type DAL struct {
	db *gorm.DB
}

func NewDAL(db *gorm.DB) *DAL {
	return &DAL{db: db}
}

func (d DAL) GetOrderByID(ctx context.Context, id string) (*po.OrderPO, error) {
	po := &po.OrderPO{}
	if err := d.db.First(po, "id = ?", id).Error; err != nil {
		return nil, err
	}
	return po, nil
}

func (d DAL) GetOrderSaleItems(ctx context.Context, orderID string) ([]*po.SaleItemPO, error) {
	pos := make([]*po.SaleItemPO, 0)
	if err := d.db.Where("id = ?", orderID).Find(&pos).Error; err != nil {
		return nil, err
	}
	return pos, nil
}

func (d DAL) GetOrderCoupons(ctx context.Context, orderID string) ([]*po.CouponPO, error) {
	pos := make([]*po.CouponPO, 0)
	if err := d.db.Where("order_id = ?", orderID).Find(&pos).Error; err != nil {
		return nil, err
	}
	return pos, nil
}

type SearchOrderOpt struct {
	UserID       *string
	CreateTimeGT *time.Time
	CreateTimeLT *time.Time
	Offset       *int32
	Limit        *int32
}

func (d DAL) GetOrderList(ctx context.Context, opt SearchOrderOpt) ([]*po.OrderPO, error) {
	db := d.db
	if opt.UserID != nil {
		db = db.Where("user = ?", *opt.UserID)
	}
	if opt.CreateTimeGT != nil {
		db = db.Where("create_time > ?", *opt.CreateTimeGT)
	}
	if opt.CreateTimeLT != nil {
		db = db.Where("create_time < ?", *opt.CreateTimeLT)
	}
	if opt.Offset != nil {
		db = db.Offset(int(*opt.Offset))
	}
	if opt.Limit != nil {
		db = db.Limit(int(*opt.Limit))
	}
	result := make([]*po.OrderPO, 0)
	if err := db.Find(&result).Error; err != nil {
		return nil, err
	}
	return result, nil
}

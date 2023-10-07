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

package infrastructure

import (
	ddd "github.com/bytedance/dddfirework"
	"github.com/bytedance/dddfirework/example/biz/sale/domain"
	"github.com/bytedance/dddfirework/example/biz/sale/infrastructure/po"
	"github.com/bytedance/dddfirework/executor/mysql"
)

func Init() {
	mysql.RegisterEntity2Model(&domain.Order{}, func(entity, parent ddd.IEntity, op ddd.OpType) (mysql.IModel, error) {
		do := entity.(*domain.Order)
		m := &po.OrderPO{
			ID:          do.GetID(),
			User:        do.UserID,
			TotalAmount: do.TotalAmount,
			Remark:      do.Remark,
		}
		// 注意，在更新 (OpUpdate)，查询 (OpQuery) 场景下，输出的 PO 字段的默认值需要保持零值状态
		// 如果需要设置插入的默认值，需要判断在插入(OpInsert)场景下进行
		if op == ddd.OpInsert {
			if do.Remark == "" {
				m.Remark = "-"
			}
		}
		return m, nil
	}, func(m mysql.IModel, do ddd.IEntity) error {
		orderPO, order := m.(*po.OrderPO), do.(*domain.Order)
		order.UserID = orderPO.User
		order.TotalAmount = orderPO.TotalAmount
		order.Remark = orderPO.Remark
		return nil
	})

	mysql.RegisterEntity2Model(&domain.SaleItem{}, func(entity, parent ddd.IEntity, op ddd.OpType) (mysql.IModel, error) {
		do := entity.(*domain.SaleItem)

		po := &po.SaleItemPO{
			ID:    do.GetID(),
			Code:  do.Code,
			Name:  do.Name,
			Price: do.Price,
			Count: do.Count,
		}
		if order, ok := parent.(*domain.Order); ok && parent != nil {
			po.OrderID = order.GetID()
		}
		return po, nil
	}, func(m mysql.IModel, do ddd.IEntity) error {
		fr, to := m.(*po.SaleItemPO), do.(*domain.SaleItem)
		to.SetID(fr.GetID())
		to.Name = fr.Name
		to.Price = fr.Price
		to.Count = fr.Count
		to.Code = fr.Code
		return nil
	})

	mysql.RegisterEntity2Model(&domain.Coupon{}, func(entity, parent ddd.IEntity, op ddd.OpType) (mysql.IModel, error) {
		do := entity.(*domain.Coupon)

		po := &po.CouponPO{
			ID:       do.GetID(),
			CouponID: do.CouponID,
			Rule:     do.Rule,
			Discount: do.Discount,
		}
		if order, ok := parent.(*domain.Order); ok && parent != nil {
			po.OrderID = order.GetID()
		}
		return po, nil
	}, func(m mysql.IModel, do ddd.IEntity) error {
		fr, to := m.(*po.CouponPO), do.(*domain.Coupon)
		to.SetID(fr.GetID())
		to.CouponID = fr.CouponID
		to.Discount = fr.Discount
		to.Rule = fr.Rule
		return nil
	})
}

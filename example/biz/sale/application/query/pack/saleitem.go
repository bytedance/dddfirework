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

package pack

import (
	"github.com/bytedance/dddfirework/example/biz/sale/infrastructure/po"
	"github.com/bytedance/dddfirework/example/common/dto/sale"
)

func MakeSaleItem(po *po.SaleItemPO) *sale.SaleItem {
	return &sale.SaleItem{
		Code:  po.Code,
		Name:  po.Name,
		Price: po.Price,
		Count: po.Count,
	}
}

func MakeSaleItemList(pos []*po.SaleItemPO) []*sale.SaleItem {
	result := make([]*sale.SaleItem, 0)
	for _, po := range pos {
		result = append(result, MakeSaleItem(po))
	}
	return result
}

func MakeCoupon(po *po.CouponPO) *sale.Coupon {
	return &sale.Coupon{
		ID:       po.ID,
		Rule:     po.Rule,
		Discount: po.Discount,
	}
}

func MakeCouponList(pos []*po.CouponPO) []*sale.Coupon {
	result := make([]*sale.Coupon, 0)
	for _, po := range pos {
		result = append(result, MakeCoupon(po))
	}
	return result
}

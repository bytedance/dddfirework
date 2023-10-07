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

type SaleItem struct {
	ddd.BaseEntity

	Code  string // 编码
	Name  string // 商品名
	Price int64  // 价格
	Count int32  // 数量

	// 新建实体的情况下，ID 是延迟生成的，于是保存order的引用，在 AfterCreate Hook 方法处获取 ID
	order *Order
}

func (s *SaleItem) AfterCreate(ctx context.Context) error {
	s.AddEvent(sale_event.NewOrderSaleItemAddedEvent(s.order.ID, s.GetID()))
	return nil
}

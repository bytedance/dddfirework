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

package po

import (
	"time"
)

type SaleItemPO struct {
	ID        string
	OrderID   string
	Code      string // 编码
	Name      string // 商品名
	Price     int64  // 价格
	Count     int32  // 数量
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt *time.Time `gorm:"index"`
}

func (s *SaleItemPO) GetID() string {
	return s.ID
}

func (s *SaleItemPO) TableName() string {
	return "example_order_item"
}

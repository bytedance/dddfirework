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

package event_handler

import (
	"context"

	ddd "github.com/bytedance/dddfirework"
	"github.com/bytedance/dddfirework/example/common/domain_event/sale"
	"github.com/bytedance/dddfirework/logger/stdr"
)

var logger = stdr.NewStdr("handler")

// OnSaleItemAddedHandler 是 MainFunc 类型
func OnSaleItemAddedHandler(ctx context.Context, repo *ddd.Repository) (err error) {
	// handle sale item added event here
	logger.Info("sale item added")

	return nil
}

func NewOnSaleItemAddedHandler(evt *sale.OrderSaleItemAddedEvent) ddd.MainFunc {
	return OnSaleItemAddedHandler
}

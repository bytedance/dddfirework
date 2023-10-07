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
	ddd "github.com/bytedance/dddfirework"
	"github.com/bytedance/dddfirework/example/common/domain_event/sale"
)

type OnSaleItemAddedHandler struct {
	ddd.Command

	event *sale.OrderSaleItemAddedEvent
}

func NewOnSaleItemAddedHandler(evt *sale.OrderSaleItemAddedEvent) *OnSaleItemAddedHandler {
	return &OnSaleItemAddedHandler{
		event: evt,
	}
}

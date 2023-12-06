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
)

// OnOrderCreatedHandler 实现了 ICommandMain 的接口
type OnOrderCreatedHandler struct {
	event *sale.OrderCreatedEvent
}

func (h *OnOrderCreatedHandler) Main(ctx context.Context, repo *ddd.Repository) (err error) {
	// handle order created event here
	logger.Info("order created")

	return nil
}

func NewOnOrderCreatedHandler(evt *sale.OrderCreatedEvent) *OnOrderCreatedHandler {
	return &OnOrderCreatedHandler{
		event: evt,
	}
}

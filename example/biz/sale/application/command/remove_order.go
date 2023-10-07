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

package command

import (
	"context"

	"github.com/bytedance/dddfirework"
	"github.com/bytedance/dddfirework/example/biz/sale/domain"
)

type DeleteOrderCommand struct {
	dddfirework.Command

	orderID string
}

func NewDeleteOrderCommand(orderID string) *DeleteOrderCommand {
	return &DeleteOrderCommand{
		orderID: orderID,
	}
}

func (c *DeleteOrderCommand) Build(ctx context.Context, builder dddfirework.DomainBuilder) (roots []dddfirework.IEntity, err error) {
	return []dddfirework.IEntity{&domain.Order{ID: c.orderID}}, nil
}

func (c *DeleteOrderCommand) Act(ctx context.Context, container dddfirework.RootContainer, roots ...dddfirework.IEntity) error {
	container.Remove(roots[0])
	return nil
}

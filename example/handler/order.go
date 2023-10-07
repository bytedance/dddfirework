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

package handler

import (
	"context"
	"time"

	"github.com/bytedance/dddfirework"
	"github.com/bytedance/dddfirework/example/biz/sale/application/command"
	"github.com/bytedance/dddfirework/example/biz/sale/application/query"
	"github.com/bytedance/dddfirework/example/biz/sale/infrastructure/dal"
)

// SaleServiceImpl implements the last service interface defined in the IDL.
type SaleServiceImpl struct {
	engine *dddfirework.Engine
}

func NewSaleService(engine *dddfirework.Engine) *SaleServiceImpl {
	return &SaleServiceImpl{engine: engine}
}

// CreateOrder implements the SaleServiceImpl interface.
func (s *SaleServiceImpl) CreateOrder(ctx context.Context, req *CreateOrderRequest) (resp *CreateOrderResponse, err error) {
	cmd := command.NewCreateOrderCommand(
		req.User, req.Items, req.Coupons,
	)
	res := s.engine.RunCommand(ctx, cmd)
	if res.Error != nil {
		return nil, res.Error
	}
	return &CreateOrderResponse{OrderID: cmd.Result.OrderID}, nil
}

// UpdateOrder implements the SaleServiceImpl interface.
func (s *SaleServiceImpl) UpdateOrder(ctx context.Context, req *UpdateOrderRequest) (resp *UpdateOrderResponse, err error) {
	if err := s.engine.RunCommand(ctx, command.NewUpdateOrderCommand(
		req.ID, command.UpdateOrderOpt{Remark: req.Remark},
	)).Error; err != nil {
		return nil, err
	}
	return &UpdateOrderResponse{}, nil
}

// DeleteOrder implements the SaleServiceImpl interface.
func (s *SaleServiceImpl) DeleteOrder(ctx context.Context, req *DeleteOrderRequest) (resp *DeleteOrderResponse, err error) {
	if err := s.engine.RunCommand(ctx, command.NewDeleteOrderCommand(
		req.ID,
	)).Error; err != nil {
		return nil, err
	}
	return &DeleteOrderResponse{}, nil
}

// GetOrder implements the SaleServiceImpl interface.
func (s *SaleServiceImpl) GetOrder(ctx context.Context, req *GetOrderRequest) (resp *GetOrderResponse, err error) {
	order, err := query.GetOrder(ctx, req.ID)
	if err != nil {
		return nil, err
	}
	return &GetOrderResponse{Order: order}, nil
}

// GetOrderList implements the SaleServiceImpl interface.
func (s *SaleServiceImpl) GetOrderList(ctx context.Context, req *GetOrderListRequest) (resp *GetOrderListResponse, err error) {
	opt := dal.SearchOrderOpt{
		UserID: req.UserID,
		Offset: req.Offset,
		Limit:  req.Limit,
	}
	if req.CreateTimeBegin != nil {
		t := time.Unix(*req.CreateTimeBegin, 0)
		opt.CreateTimeGT = &t
	}
	if req.CreateTimeEnd != nil {
		t := time.Unix(*req.CreateTimeEnd, 0)
		opt.CreateTimeLT = &t
	}

	orders, err := query.GetOrderList(ctx, opt)
	if err != nil {
		return nil, err
	}
	return &GetOrderListResponse{Items: orders}, nil
}

// AddSaleItem implements the SaleServiceImpl interface.
func (s *SaleServiceImpl) AddSaleItem(ctx context.Context, req *AddSaleItemRequest) (resp *AddSaleItemResponse, err error) {
	if err := s.engine.RunCommand(ctx, command.NewAddSaleItemCommand(
		req.OrderID, req.Item,
	)).Error; err != nil {
		return nil, err
	}
	return &AddSaleItemResponse{}, nil
}

// AddCoupon implements the SaleServiceImpl interface.
func (s *SaleServiceImpl) AddCoupon(ctx context.Context, req *AddCouponRequest) (resp *AddCouponResponse, err error) {
	if err := s.engine.RunCommand(ctx, command.NewAddCouponCommand(
		req.OrderID, req.Coupon,
	)).Error; err != nil {
		return nil, err
	}
	return &AddCouponResponse{}, nil
}

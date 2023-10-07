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

package example_outer

import (
	"context"
	"time"

	"github.com/bytedance/dddfirework"
	event_bus "github.com/bytedance/dddfirework/eventbus/mysql"
	"github.com/bytedance/dddfirework/example/common/domain_event/sale"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var eventBus *event_bus.EventBus
var db *gorm.DB

func Init() {
	dsn := "root:@tcp(localhost:3308)/my_db?parseTime=true"
	mysqlDB, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic(err)
	}
	db = mysqlDB
	ctx := context.Background()
	// 初始化 eventBus，需要保证和框架用的是同一个 eventBus
	eventBus = event_bus.NewEventBus("gateway", db, func(opt *event_bus.Options) {
		// 设定回查的超时时间
		opt.TXCheckTimeout = time.Minute
	})
	// 设定事务超时回查接口
	eventBus.RegisterEventTXChecker(func(evt *dddfirework.DomainEvent) dddfirework.TXStatus {
		return dddfirework.TXCommit
	})

	// 注册 eventBus
	dddfirework.RegisterEventBus(eventBus)
	// 注册事件处理器
	dddfirework.RegisterEventHandler(sale.EventOrderCreated, func(ctx context.Context, evt *sale.OrderCreatedEvent) (err error) {
		// do something
		return
	})
	// 启动
	eventBus.Start(ctx)
}

// Handle 外部业务处理函数
func Handle(ctx context.Context) {
	var eb = eventBus
	err := db.Transaction(func(tx *gorm.DB) error {
		var err error
		// do something
		// 此处可以处理业务逻辑

		// 发送事件，事件会被所有引用该 eventBus 的框架接受并处理
		ctx, err = eb.DispatchBegin(ctx, dddfirework.NewDomainEvent(
			&sale.OrderCreatedEvent{OrderID: "123"},
			dddfirework.WithSendType(dddfirework.SendTypeTransaction),
		))
		return err
	})

	// 事务保存后的回调
	if err == nil {
		_ = eb.Commit(ctx)
	} else {
		_ = eb.Rollback(ctx)
	}

}

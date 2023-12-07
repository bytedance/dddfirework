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

package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/bytedance/dddfirework"
	db_eventbus "github.com/bytedance/dddfirework/eventbus/mysql"
	"github.com/bytedance/dddfirework/example/biz/sale/application/query"
	"github.com/bytedance/dddfirework/example/biz/sale/infrastructure"
	"github.com/bytedance/dddfirework/example/biz/sale/infrastructure/po"
	"github.com/bytedance/dddfirework/example/event_handler"
	"github.com/bytedance/dddfirework/example/handler"
	"github.com/bytedance/dddfirework/example/handler/util"
	db_executor "github.com/bytedance/dddfirework/executor/mysql"
	db_lock "github.com/bytedance/dddfirework/lock/db"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func initPO(db *gorm.DB) {
	// 可选，如果使用 mysql 实现 eventbus，需要提前建表。
	if err := db.AutoMigrate(&db_eventbus.EventPO{}, &db_eventbus.Transaction{}, &db_eventbus.ServicePO{}); err != nil {
		panic(err)
	}

	if err := db.AutoMigrate(&po.OrderPO{}, &po.CouponPO{}, &po.SaleItemPO{}); err != nil {
		panic(err)
	}
}

func main() {
	dsn := "root:@tcp(localhost:3308)/my_db?parseTime=true"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic(err)
	}
	initPO(db)

	lock := db_lock.NewDBLock(db, time.Second*10)
	executor := db_executor.NewExecutor(db)
	eventBus := db_eventbus.NewEventBus("svc_example", db)
	eventBus.Start(context.Background())

	engine := dddfirework.NewEngine(lock, executor, eventBus.Options()...)

	query.Init(db)
	infrastructure.Init()
	event_handler.Register(engine)
	service := handler.NewSaleService(engine)
	http.HandleFunc("/", util.Handler(service))
	log.Fatal(http.ListenAndServe(":8080", nil))
}

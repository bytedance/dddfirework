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

package testsuit

import (
	"fmt"
	"os"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type MySQLOption struct {
	NoLog bool
}

// InitMysql 启动测试数据库
// 前提: 在根目录执行 docker-compose up 命令
func InitMysql(opts ...MySQLOption) *gorm.DB {
	dsn := "root:@tcp(mysql:3306)/my_db?parseTime=true&loc=Local"
	if os.Getenv("LOCAL_TEST") == "true" {
		dsn = "root:@tcp(localhost:3308)/my_db?parseTime=true&loc=Local"
	}
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	return db.Debug()
}

func InitMysqlWithDatabase(db *gorm.DB, database string) *gorm.DB {
	err := db.Exec("CREATE DATABASE IF NOT EXISTS " + database + ";").Error // ignore_security_alert
	if err != nil {
		panic(err)
	}
	dsn := fmt.Sprintf("root:@tcp(mysql:3306)/%s?parseTime=true&loc=Local", database)
	if os.Getenv("LOCAL_TEST") == "true" {
		dsn = fmt.Sprintf("root:@tcp(localhost:3308)/%s?parseTime=true&loc=Local", database)
	}
	ndb, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic(err)
	}
	return ndb.Debug()
}

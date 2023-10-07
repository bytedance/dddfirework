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

package db

import (
	"time"
)

// ResourceLock is the model of ResourceLock.
type ResourceLock struct {
	ID       uint   `gorm:"primarykey;AUTO_INCREMENT"`
	Resource string `gorm:"type:varchar(255);unique"`
	// 假设一种情况，A请求拿到了锁，但是本身任务太长，超过了锁时间，B请求拿到了锁，并且开始执行任务，A任务完成了会去解锁，这个时候就意外地把B刚拿到的锁解了
	// Lock成功应该生成一个所有者ID，保存到表里面并且返回，unlock的时候需要指定这个所有者ID匹配了去解锁，这样可以防止解锁了其他人的锁
	LockerID  string `gorm:"type:varchar(255);index:idx_locker_id"`
	CreatedAt time.Time
	UpdatedAt time.Time
}

// TableName returns the table name of the Partition.
func (ResourceLock) TableName() string {
	return "resource_lock"
}

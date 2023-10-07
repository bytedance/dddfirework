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

package mysql

import (
	"time"

	"github.com/bytedance/dddfirework"
)

type EventStatus int8

const (
	EventStatusToSend EventStatus = 1
	EventStatusSent   EventStatus = 2
	EventStatusFailed EventStatus = 3
)

// EventPO 事件存储模型
/*
CREATE TABLE `ddd_domain_event` (
   `id` int NOT NULL AUTO_INCREMENT,
   `event_id` varchar(64) NOT NULL,
   `event` text NOT NULL,
   `trans_id` int,
   `event_created_at` datetime(3) DEFAULT NULL,
   `created_at` datetime(3) DEFAULT NULL,
   PRIMARY KEY (`id`),
   KEY `idx_ddd_domain_event_event_id` (`event_id`),
   KEY `idx_ddd_domain_event_trans_id` (`trans_id`),
   KEY `idx_ddd_domain_event_created_at` (`created_at`),
   KEY `idx_ddd_domain_event_event_created_at` (`event_created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
*/
type EventPO struct {
	ID             int64                    `gorm:"primaryKey;autoIncrement"`
	EventID        string                   `gorm:"column:event_id"`
	Event          *dddfirework.DomainEvent `gorm:"serializer:json"`
	TransID        int64                    `gorm:"column:trans_id"` // 事务id
	EventCreatedAt time.Time                `gorm:"index"`           // 事件的创建时间
	CreatedAt      time.Time                `gorm:"index"`           // 记录创建时间
}

func (o *EventPO) TableName() string {
	return "ddd_domain_event"
}

/*
CREATE TABLE `ddd_event_transaction` (

	`id` int NOT NULL AUTO_INCREMENT,
	`service` varchar(30) NOT NULL,
	`events` text,
	`due_time` datetime(3) DEFAULT NULL,
	`created_at` datetime(3) DEFAULT NULL,
	PRIMARY KEY (`id`),
	KEY `idx_ddd_event_transaction_created_at` (`created_at`)

) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
*/
type Transaction struct {
	ID        int64                      `gorm:"primaryKey;autoIncrement"`
	Service   string                     `gorm:"column:service"` // 服务名
	Events    []*dddfirework.DomainEvent `gorm:"serializer:json"`
	DueTime   time.Time                  `gorm:"column:due_time"` // 事务超时时间
	CreatedAt time.Time                  `gorm:"index"`           // 记录创建时间
}

func (o *Transaction) TableName() string {
	return "ddd_event_transaction"
}

type RetryInfo struct {
	ID         int64
	RetryCount int       // 第 RetryCount 次重试， 有限范围从 1 开始， 0 表示初始状态
	RetryTime  time.Time // 重试时间
}

type FailedInfo struct {
	IDs   []string
	Retry int
}

// ServicePO 服务存储模型
/*
	CREATE TABLE `ddd_eventbus_service` (
	`name` varchar(30) NOT NULL,
	`failed` text,
	`retry` text,
	`offset` bigint(20) DEFAULT NULL,
	`created_at` datetime(3) DEFAULT NULL,
	`updated_at` datetime(3) DEFAULT NULL,
	PRIMARY KEY (`name`),
	KEY `idx_ddd_eventbus_service_created_at` (`created_at`),
	KEY `idx_ddd_eventbus_service_updated_at` (`updated_at`)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
*/
type ServicePO struct {
	Name      string       `gorm:"primaryKey"`
	Retry     []*RetryInfo `gorm:"serializer:json"` // 重试信息
	Failed    []*RetryInfo `gorm:"serializer:json"` // 失败信息
	Offset    int64        `gorm:"column:offset"`   // 消费位置，等于最后一次消费的事件id
	CreatedAt time.Time    `gorm:"index"`           // 记录创建时间
	UpdatedAt time.Time    `gorm:"index"`           // 记录的更新时间
}

func (o *ServicePO) GetID() string {
	return o.Name
}

func (o *ServicePO) TableName() string {
	return "ddd_eventbus_service"
}

func eventPersist(event *dddfirework.DomainEvent) (*EventPO, error) {
	return &EventPO{
		EventID:        event.ID,
		Event:          event,
		EventCreatedAt: event.CreatedAt,
	}, nil
}

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

type ServiceEventStatus int8

const (
	ServiceEventStatusInit    ServiceEventStatus = 0
	ServiceEventStatusSuccess ServiceEventStatus = 10
	ServiceEventStatusFailed  ServiceEventStatus = 21
	// ServiceEventStatusPrecedingFailed 前置event失败
	ServiceEventStatusPrecedingFailed ServiceEventStatus = 22
	// ServiceEventStatusExpired 过期，在保序事件中，一个event处理失败，会阻塞后序同名sender_id的处理，但不能永远阻塞，可以通过人工改db或其他策略将已经失败的几个event置为过期，不影响新的同名sender_id event的执行
	ServiceEventStatusExpired ServiceEventStatus = 31
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
   KEY `idx_ddd_domain_event_sender` (`sender`),
   KEY `idx_ddd_domain_event_trans_id` (`trans_id`),
   KEY `idx_ddd_domain_event_created_at` (`created_at`),
   KEY `idx_ddd_domain_event_event_created_at` (`event_created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
*/
type EventPO struct {
	ID             int64                    `gorm:"primaryKey;autoIncrement"`
	EventID        string                   `gorm:"column:event_id"`
	Sender         string                   `gorm:"type:varchar(255);column:sender;index"` // 事件发出实体 ID
	Event          *dddfirework.DomainEvent `gorm:"serializer:json"`
	TransID        int64                    `gorm:"column:trans_id"` // 事务id
	EventCreatedAt time.Time                `gorm:"index"`           // 事件的创建时间
	CreatedAt      time.Time                `gorm:"index"`           // 记录创建时间
}

func (o *EventPO) TableName() string {
	return "ddd_domain_event"
}

// Transaction
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
	`created_at` datetime(3) DEFAULT NULL,
	`updated_at` datetime(3) DEFAULT NULL,
	PRIMARY KEY (`name`),
	KEY `idx_ddd_eventbus_service_created_at` (`created_at`),
	KEY `idx_ddd_eventbus_service_updated_at` (`updated_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
*/
type ServicePO struct {
	Name      string    `gorm:"primaryKey"`
	CreatedAt time.Time `gorm:"index"` // 记录创建时间
	UpdatedAt time.Time `gorm:"index"` // 记录的更新时间
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
		Sender:         event.Sender,
		Event:          event,
		EventCreatedAt: event.CreatedAt,
	}, nil
}

/*
CREATE TABLE `ddd_domain_service_event` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `service` varchar(30) DEFAULT NULL,
  `event_id` bigint NOT NULL,
  `retry_count` int(11) DEFAULT NULL,
  `status` tinyint DEFAULT NULL,
  `failed_message` text,
  `event_created_at` datetime(3) NOT NULL,
  `next_time` datetime(3) NOT NULL,
  `run_at` datetime(3) DEFAULT '1970-01-01 00:00:01',
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_service_event_id` (`service`,`event_id`),
  KEY `idx_service_status_next_time` (`service`,`status`,`next_time`),
  KEY `idx_service_status_event_id` (`service`,`status`,`event_id`),
  KEY `idx_ddd_domain_service_event_event_created_at` (`event_created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
*/

// ServiceEventPO 记录service对event的处理情况
type ServiceEventPO struct {
	ID             int64              `gorm:"primaryKey;autoIncrement"`
	Service        string             `gorm:"type:varchar(30);column:service;uniqueIndex:idx_service_event_id;index:idx_service_status_event_id;index:idx_service_status_next_time;"`
	EventID        int64              `gorm:"column:event_id;uniqueIndex:idx_service_event_id;index:idx_service_status_event_id;not null"`
	RetryCount     int                `gorm:"type:int(11);column:retry_count"`                                                                  // 重试次数
	Status         ServiceEventStatus `gorm:"type:tinyint;column:status;index:idx_service_status_event_id;index:idx_service_status_next_time;"` // service event状态
	FailedMessage  string             `gorm:"type:text;column:failed_message"`                                                                  // 失败详情
	EventCreatedAt time.Time          `gorm:"type:datetime(3);index;not null"`                                                                  // 事件的创建时间
	NextTime       time.Time          `gorm:"type:datetime(3);index:idx_service_status_next_time;not null"`                                     // 事件可以运行的事件
	RunAt          time.Time          `gorm:"type:datetime(3);zeroValue:1970-01-01 00:00:01;default:'1970-01-01 00:00:01+00:00'"`               // 事件真实运行时间，单纯记录下
}

func (o *ServiceEventPO) TableName() string {
	return "ddd_domain_service_event"
}

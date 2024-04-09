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
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/avast/retry-go"
	"github.com/go-logr/logr"
	"github.com/rs/xid"
	"gorm.io/gorm"

	"github.com/bytedance/dddfirework"
	"github.com/bytedance/dddfirework/logger/stdr"
)

var defaultLogger = stdr.NewStdr("resource_lock")

const (
	// 定时协程每隔renewInterval 去续期，renewInterval必须小于ttl，以确保在本次ttl到期前，续期定时协程能及时续上。
	renewInterval = 1 * time.Second
)

type Options struct {
	RenewInterval time.Duration
	Retry         bool
	Logger        logr.Logger
}

type Option func(opt *Options)

type DBLock struct {
	ttl    time.Duration
	db     *gorm.DB
	logger logr.Logger
	opt    Options
}

func NewDBLock(db *gorm.DB, ttl time.Duration, options ...Option) *DBLock {
	opt := Options{
		RenewInterval: renewInterval,
		Retry:         true,
		Logger:        defaultLogger,
	}
	for _, o := range options {
		o(&opt)
	}
	if ttl < opt.RenewInterval {
		panic(fmt.Sprintf("ttl can not less than %f seconds", opt.RenewInterval.Seconds()))
	}
	return &DBLock{db: db, ttl: ttl, logger: opt.Logger, opt: opt}
}

func (r *DBLock) Lock(ctx context.Context, key string) (keyLock interface{}, err error) {
	if !r.opt.Retry {
		keyLock, err = r.lock(ctx, key)
		return
	}
	// 加锁失败后重试
	err = retry.Do(
		func() error {
			keyLock, err = r.lock(ctx, key)
			return err
		},
		retry.RetryIf(func(err error) bool { // 只针对 dddfirework.ErrEntityLocked 重试
			return err == dddfirework.ErrEntityLocked
		}),
		retry.DelayType(retry.FixedDelay),
		retry.Delay(100*time.Millisecond), // 重试间隔
		retry.Attempts(5),                 // 重试次数
		retry.LastErrorOnly(true),
	)
	return
}

func (r *DBLock) lock(ctx context.Context, key string) (keyLock interface{}, err error) {
	lockerID := xid.New().String()
	var lock ResourceLock
	err = r.db.WithContext(ctx).Model(&ResourceLock{}).
		Where("`resource` = ?", key).First(&lock).
		Error
	if err != nil {
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("failed to get resource %s lock, err: %w", key, err)
		}
		// 如果没有记录，也就是没有锁
		l := &ResourceLock{Resource: key, LockerID: lockerID}
		err = r.db.WithContext(ctx).Create(l).Error
		if err != nil {
			if errors.Is(err, gorm.ErrDuplicatedKey) {
				return nil, dddfirework.ErrEntityLocked
			}
			return nil, fmt.Errorf("failed to create resource %s lock, err: %w", key, err)
		}
		return l, nil
	}
	// locked
	if time.Since(lock.UpdatedAt) < r.ttl {
		// key is locked
		return nil, dddfirework.ErrEntityLocked
	}
	// 有记录但是数据过期
	res := r.db.WithContext(ctx).Model(&ResourceLock{}).Where("resource = ?", key).
		Where("locker_id = ?", lock.LockerID).
		UpdateColumns(ResourceLock{UpdatedAt: time.Now(), LockerID: lockerID})

	if res.Error != nil {
		return nil, fmt.Errorf("failed to update resource %s lock: %w", key, res.Error)
	}
	if res.RowsAffected == 0 {
		// resource updated by others
		return nil, dddfirework.ErrEntityLocked
	}
	lock.LockerID = lockerID
	return &lock, nil
}

func (r *DBLock) UnLock(ctx context.Context, keyLock interface{}) error {
	l := keyLock.(*ResourceLock)
	res := r.db.WithContext(ctx).Where("locker_id = ? and resource = ?", l.LockerID, l.Resource).Delete(&ResourceLock{})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected <= 0 { // should not go into this
		return fmt.Errorf("lock record not found (id=%s resource=%s)", l.LockerID, l.Resource)
	}
	return nil
}

func (r *DBLock) update(ctx context.Context, keyLock interface{}) error {
	l := keyLock.(*ResourceLock)
	res := r.db.WithContext(ctx).
		Model(&ResourceLock{}).
		Where("`resource` = ? AND `locker_id` = ?", l.Resource, l.LockerID). // check if locker_id has been changed by others
		UpdateColumns(ResourceLock{UpdatedAt: time.Now(), LockerID: l.LockerID})
	if res.Error != nil {
		return fmt.Errorf("failed to update resource %s lock: %w", l.Resource, res.Error)
	}
	if res.RowsAffected == 0 {
		return fmt.Errorf("resource %s updated by others", l.Resource)
	}
	return nil
}

func (r *DBLock) Run(ctx context.Context, key string, fn func(ctx context.Context)) error {
	locker, err := r.Lock(ctx, key)
	if err != nil {
		return err
	}
	defer func() {
		if err = r.UnLock(ctx, locker); err != nil {
			r.logger.Error(err, fmt.Sprintf("failed to unlock %s", key))
		}
	}() // pass parent ctx in here, or defer will use sub ctx below

	ticker := time.NewTicker(r.opt.RenewInterval)
	// 业务函数执行完成后，会停止renew 协程
	defer ticker.Stop()
	subCtx, cancel := context.WithCancel(ctx)
	go func() {
		for range ticker.C {
			if err = r.update(ctx, locker); err != nil {
				// 续期失败，会通知业务函数停止执行
				cancel()
				r.logger.Info(fmt.Sprintf("failed to renew lock %s, error: %v", key, err))
				break
			}
		}
	}()

	fn(subCtx)
	return nil
}

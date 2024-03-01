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
	"github.com/rs/xid"
	"gorm.io/gorm"

	"github.com/bytedance/dddfirework"
)

type DBLock struct {
	ttl time.Duration
	db  *gorm.DB
}

func NewDBLock(db *gorm.DB, ttl time.Duration) *DBLock {
	return &DBLock{db: db, ttl: ttl}
}

func (r *DBLock) Lock(ctx context.Context, key string) (keyLock interface{}, err error) {
	err = retry.Do(func() error {
		keyLock, err = r.lock(ctx, key)
		return err
	},
		retry.RetryIf(func(err error) bool { // 只针对 dddfirework.ErrEntityLocked 重试
			return err == dddfirework.ErrEntityLocked
		}),
		retry.Delay(100*time.Millisecond), // 重试间隔
		retry.Attempts(5),                 // 重试次数
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
		Where("updated_at = ?", lock.UpdatedAt).
		UpdateColumns(ResourceLock{UpdatedAt: time.Now(), LockerID: lockerID})

	if res.Error != nil {
		return nil, fmt.Errorf("failed to update resource %s lock: %w", key, res.Error)
	}
	if res.RowsAffected == 0 {
		// resource updated by others
		return nil, dddfirework.ErrEntityLocked
	}
	return &lock, nil
}

func (r *DBLock) UnLock(ctx context.Context, keyLock interface{}) error {
	l := keyLock.(*ResourceLock)
	err := r.db.WithContext(ctx).Where("locker_id = ? and resource = ?", l.LockerID, l.Resource).Delete(&ResourceLock{}).Error
	if err != nil {
		return err
	}
	return nil
}

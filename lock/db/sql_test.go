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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestLock(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	assert.NoError(t, err)
	err = db.AutoMigrate(&ResourceLock{})
	assert.NoError(t, err)
	lock := NewDBLock(db.Debug(), 1*time.Second)
	l, err := lock.Lock(context.Background(), "abc")
	assert.NoError(t, err)
	r := l.(*ResourceLock)
	assert.True(t, len(r.LockerID) > 0)

	// 测试lock 过期场景
	time.Sleep(1 * time.Second)
	previousID := r.LockerID
	l, err = lock.Lock(context.Background(), "abc")
	assert.NoError(t, err)
	assert.NotEqual(t, previousID, l.(*ResourceLock).LockerID)
	err = lock.UnLock(context.Background(), l)
	assert.NoError(t, err)
}

func TestUnLock(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	assert.NoError(t, err)
	err = db.AutoMigrate(&ResourceLock{})
	assert.NoError(t, err)
	lock := NewDBLock(db.Debug(), 1*time.Second)
	var l interface{}
	l, err = lock.Lock(context.Background(), "abc")
	assert.NoError(t, err)
	err = lock.UnLock(context.Background(), l)
	assert.NoError(t, err)
	_, err = lock.Lock(context.Background(), "abc")
	assert.NoError(t, err)
}

func TestRun(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	assert.NoError(t, err)
	err = db.AutoMigrate(&ResourceLock{})
	assert.NoError(t, err)
	var wg sync.WaitGroup
	wg.Add(1)
	lock := NewDBLock(db.Debug(), 5*time.Second, func(opt *Options) {
		opt.Retry = false
	})
	go func() {
		err = lock.Run(context.Background(), "abc", func(ctx context.Context) {
			defer wg.Done()
			time.Sleep(2 * time.Second)
		})
	}()
	time.Sleep(1 * time.Second)
	// 会加锁失败
	_, err = lock.Lock(context.Background(), "abc")
	fmt.Println(err)
	assert.Error(t, err)
	wg.Wait()
}

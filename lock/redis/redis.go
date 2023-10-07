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

package redis

import (
	"context"
	"time"

	"github.com/bsm/redislock"
	"github.com/redis/go-redis/v9"
)

type RedisLock struct {
	ttl time.Duration
	cli *redislock.Client
}

func NewRedisLock(cli *redis.Client, ttl time.Duration) *RedisLock {
	return &RedisLock{cli: redislock.New(cli), ttl: ttl}
}

func (r *RedisLock) Lock(ctx context.Context, key string) (keyLock interface{}, err error) {
	return r.cli.Obtain(ctx, key, r.ttl, &redislock.Options{
		// 默认固定间隔重试，最大重试30次
		RetryStrategy: redislock.LimitRetry(redislock.LinearBackoff(100*time.Millisecond), 30)},
	)
}

func (r *RedisLock) UnLock(ctx context.Context, keyLock interface{}) error {
	l := keyLock.(*redislock.Lock)
	return l.Release(ctx)
}

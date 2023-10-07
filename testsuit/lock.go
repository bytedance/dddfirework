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
	"context"
)

type MemLock struct {
}

func NewMemLock() *MemLock {
	return &MemLock{}
}

func (l *MemLock) Lock(ctx context.Context, key string) (keyLock interface{}, err error) {
	return key, nil
}

func (l *MemLock) UnLock(ctx context.Context, keyLock interface{}) error {
	return nil
}

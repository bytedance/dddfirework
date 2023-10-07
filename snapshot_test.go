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

package dddfirework

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type entityA struct {
	BaseEntity
}

type entityB struct {
	BaseEntity

	A     *entityA
	A2    *entityA
	AList []*entityA
	AMap  map[string]*entityA
}

func TestFindChildren(t *testing.T) {
	b := entityB{
		BaseEntity: NewBase("B1"),
		A:          &entityA{BaseEntity: NewBase("A3")},
		AList: []*entityA{{
			BaseEntity: NewBase("A1"),
		}},
		AMap: map[string]*entityA{"A": {
			BaseEntity: NewBase("A2"),
		}},
	}
	children := findChildren(&b)

	assert.Greater(t, len(children), 0)

	assert.Contains(t, children, "AList")
	assert.Contains(t, children, "AMap")
	assert.Contains(t, children, "A")
	assert.Equal(t, len(children["A"]), 1)
	assert.Equal(t, len(children["AList"]), 1)
	assert.Equal(t, len(children["AMap"]), 1)

}

func TestEntityDiff(t *testing.T) {
	hasChanged := func(changed []*entityChanged, parent IEntity, key string, t changeType, child IEntity) bool {
		for _, item := range changed {
			if item.parent == parent && item.key == key && item.changeType == t {
				for _, c := range item.children {
					if c == child {
						return true
					}
				}
			}
		}
		return false
	}

	a1 := &entityA{BaseEntity: NewBase("A1")}
	a2 := &entityA{BaseEntity: NewBase("A2")}
	a3 := &entityA{BaseEntity: NewBase("A3")}
	a4 := &entityA{BaseEntity: NewBase("A4")}

	b := &entityB{
		BaseEntity: NewBase("B1"),
		A:          a1,
		AList:      []*entityA{a2, a3, a4},
		AMap:       map[string]*entityA{"A3": a3},
	}

	pool := entitySnapshotPool{}
	recordEntityChildren(b, pool)

	b.A = nil                // 删除子实体
	b.A2 = a1                // 新增子实体
	b.AList = []*entityA{a1} // 删除3个，新增1个
	b.AMap["A4"] = a4        // 在map里面新增一个
	delete(b.AMap, "A3")     // map删除一个
	changed := entityDiff(b, pool)

	assert.True(t, hasChanged(changed, b, "A", deleteChildren, a1))
	assert.True(t, hasChanged(changed, b, "A2", newChildren, a1))
	assert.True(t, hasChanged(changed, b, "AList", deleteChildren, a2))
	assert.True(t, hasChanged(changed, b, "AList", deleteChildren, a3))
	assert.True(t, hasChanged(changed, b, "AList", deleteChildren, a4))
	assert.True(t, hasChanged(changed, b, "AList", newChildren, a1))
	assert.True(t, hasChanged(changed, b, "AMap", newChildren, a4))
	assert.True(t, hasChanged(changed, b, "AMap", deleteChildren, a3))

	pool = entitySnapshotPool{}
	recordEntityChildren(b, pool)

	// 清空所有实体
	b.A2 = nil
	b.AList = nil
	b.AMap = nil

	changed = entityDiff(b, pool)
	assert.True(t, hasChanged(changed, b, "A2", deleteChildren, a1))
	assert.True(t, hasChanged(changed, b, "AList", deleteChildren, a1))
	assert.True(t, hasChanged(changed, b, "AMap", deleteChildren, a4))
}

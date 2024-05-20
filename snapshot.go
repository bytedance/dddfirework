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
	"fmt"
	"reflect"
	"sort"
)

type changeType int8

const (
	newChildren changeType = iota
	dirtyChildren
	deleteChildren
	clearChildren // 清空某个key下的所有子实体
)

type entitySnapshot struct {
	po       IModel // 当前实体映射的 PO
	children map[string][]IEntity
}

type entitySnapshotPool map[IEntity]*entitySnapshot

// 实体变更
type entityChanged struct {
	changeType changeType
	parent     IEntity // 父实体
	key        string
	children   []IEntity // 有变更的子实体
}

func (c *entityChanged) Remove(entity IEntity) bool {
	i := 0
	for _, item := range c.children {
		if item != entity {
			c.children[i] = item
			i++
		}
	}
	if i == len(c.children) {
		return false
	}
	c.children = c.children[:i]
	return true
}

type simpleSet map[IEntity]int

func (s simpleSet) Diff(s2 simpleSet) simpleSet {
	res := simpleSet{}
	for e := range s {
		if _, in := s2[e]; !in {
			res[e] = s[e]
		}
	}
	return res
}

func (s simpleSet) IsSubset(s2 simpleSet) bool {
	for e := range s {
		if _, in := s2[e]; !in {
			return false
		}
	}

	return true
}

func (s simpleSet) Inter(s2 simpleSet) simpleSet {
	res := simpleSet{}
	for e := range s {
		if _, in := s2[e]; in {
			res[e] = s[e]
		}
	}

	return res
}

func (s simpleSet) ToSlice() []IEntity {
	res := make([]IEntity, len(s))
	i := 0
	for e := range s {
		res[i] = e
		i++
	}
	sort.SliceStable(res, func(i, j int) bool {
		return s[res[i]] < s[res[j]]
	})
	return res
}

func (s simpleSet) Empty() bool {
	return len(s) == 0
}

func makeEntitySet(nodes []IEntity) simpleSet {
	s := simpleSet{}
	for i, n := range nodes {
		s[n] = i
	}
	return s
}

func recordEntityChildren(entity IEntity, pool entitySnapshotPool) {
	_ = walk(entity, nil, func(entity, parent IEntity, children map[string][]IEntity) error {
		if _, in := pool[entity]; in {
			return nil
		}

		pool[entity] = &entitySnapshot{
			children: children,
		}
		return nil
	})
}

// findChildren 查找子实体，优先使用定义的 GetChildren 方法，然后使用 reflect 去发现子实体
// 默认逻辑只处理类型为 IEntity 或者形如 []IEntity 和 map[any]IEntity 的子实体
func findChildren(entity IEntity) map[string][]IEntity {
	children := entity.GetChildren()
	if children != nil {
		return children
	}

	val := reflect.ValueOf(entity)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return nil
	}
	children = make(map[string][]IEntity)
	valType := val.Type()
	for i := 0; i < val.NumField(); i++ {
		fieldName := valType.Field(i).Name
		if _, in := children[fieldName]; in {
			continue
		}

		fieldVal := val.Field(i)
		fieldType := fieldVal.Type()
		// 非导出字段不处理
		if !fieldVal.CanInterface() {
			continue
		}
		if fieldType.Implements(entityType) && fieldType != baseEntityType {
			if fieldVal.IsNil() {
				children[fieldName] = []IEntity{}
			} else {
				children[fieldName] = []IEntity{fieldVal.Interface().(IEntity)}
			}
		} else if fieldType.Kind() == reflect.Slice && fieldType.Elem().Implements(entityType) {
			if fieldVal.Len() == 0 {
				children[fieldName] = []IEntity{}
			} else {
				for j := 0; j < fieldVal.Len(); j++ {
					children[fieldName] = append(children[fieldName], fieldVal.Index(j).Interface().(IEntity))
				}
			}
		} else if fieldType.Kind() == reflect.Map && fieldType.Elem().Implements(entityType) {
			if fieldVal.Len() == 0 {
				children[fieldName] = []IEntity{}
			} else {
				for _, j := range fieldVal.MapKeys() {
					children[fieldName] = append(children[fieldName], fieldVal.MapIndex(j).Interface().(IEntity))
				}
			}
		}
	}
	return children
}

// walk 深度优先遍历实体以及其子实体
func walk(entity, parent IEntity, f func(entity, parent IEntity, children map[string][]IEntity) error) error {
	children := findChildren(entity)
	if err := f(entity, parent, children); err != nil {
		return err
	}

	for _, entities := range children {
		for _, c := range entities {
			if err := walk(c, entity, f); err != nil {
				return err
			}
		}
	}
	return nil
}

// entityDiff 递归对比实体所有层级的子实体，生成子实体的差异
func entityDiff(parent IEntity, pool entitySnapshotPool) []*entityChanged {
	res := make([]*entityChanged, 0)
	childrenList := findChildren(parent)
	keys := []string{}
	for key := range childrenList {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		children := childrenList[key]
		s1 := makeEntitySet(children)
		s2 := simpleSet{}
		if pool[parent] != nil && len(pool[parent].children[key]) > 0 {
			s2 = makeEntitySet(pool[parent].children[key])
		}
		created, remain, deleted := s1.Diff(s2).ToSlice(), s1.Inter(s2).ToSlice(), s2.Diff(s1).ToSlice()
		if len(created) > 0 {
			res = append(res, &entityChanged{
				changeType: newChildren,
				parent:     parent,
				key:        key,
				children:   created,
			})
			for _, item := range created {
				res = append(res, entityDiff(item, pool)...)
			}
		}

		// 处理有变更的子实体
		dirty := make([]IEntity, 0)
		for _, item := range remain {
			if item.IsDirty() {
				dirty = append(dirty, item)
			}
		}
		if len(dirty) > 0 {
			res = append(res, &entityChanged{
				changeType: dirtyChildren,
				parent:     parent,
				key:        key,
				children:   dirty,
			})
		}
		for _, item := range remain {
			res = append(res, entityDiff(item, pool)...)
		}

		// 处理删除的子实体
		if len(deleted) > 0 {
			res = append(res, &entityChanged{
				changeType: deleteChildren,
				parent:     parent,
				key:        key,
				children:   deleted,
			})
		}

	}
	return res
}

func handleEntityMove(changedItems []*entityChanged) ([]*entityChanged, error) {
	// 按照 new, dirty, delete 的顺序排序
	sort.SliceStable(changedItems, func(i, j int) bool {
		return changedItems[i].changeType < changedItems[j].changeType
	})

	entities := map[IEntity]*entityChanged{}
	for _, item := range changedItems {
		for _, entity := range item.children {
			prev, ok := entities[entity]
			if !ok {
				entities[entity] = item
				continue
			}
			// 同个实体从某个父实体删除，在另一个父实体新增，判定为更新操作
			if prev.changeType == newChildren && item.changeType == deleteChildren {
				item.Remove(entity)
				prev.Remove(entity)
				changedItems = append(changedItems, &entityChanged{
					changeType: dirtyChildren,
					parent:     prev.parent,
					key:        prev.key,
					children:   []IEntity{entity},
				})
				// 考虑需要判定一个新增，多个删除的情况，这里更新一下记录
				entities[entity] = item
			} else {
				// 其他的一个子实体出现在多个父实体变更中的情况应该报错
				return nil, fmt.Errorf("entity(id=%s type=%v) repeated", entity.GetID(), reflect.TypeOf(entity))
			}
		}
	}
	return changedItems, nil
}

func recursiveDelete(changed []*entityChanged) []*entityChanged {
	for _, item := range changed {
		if item.changeType == deleteChildren {
			for _, c := range item.children {
				if err := walk(c, item.parent, func(entity, parent IEntity, children map[string][]IEntity) error {
					for key, entities := range children {
						changed = append(changed, &entityChanged{
							changeType: deleteChildren,
							parent:     entity,
							key:        key,
							children:   entities,
						})
					}
					return nil
				}); err != nil {
					return nil
				}
			}
		}
	}

	return changed
}

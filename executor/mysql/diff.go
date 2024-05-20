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
	"reflect"
	"strings"
	"sync"
)

var schemaCache = &sync.Map{}

func hasValue(tag, attr string) bool {
	parts := strings.Split(tag, ";")
	for _, p := range parts {
		if strings.HasPrefix(strings.ToUpper(p), strings.ToUpper(attr)) {
			return true
		}
	}
	return false
}

func diffStruct(currVal, prevVal reflect.Value) []string {
	result := make([]string, 0)
	poType := currVal.Type()
	for i := 0; i < currVal.NumField(); i++ {
		field := poType.Field(i)
		fieldVal, prevFiledVal := currVal.Field(i), prevVal.Field(i)
		if !fieldVal.CanInterface() {
			continue
		}
		fieldName := field.Name
		fieldTag := field.Tag.Get("gorm")
		if fieldVal.Kind() == reflect.Struct && (field.Anonymous || hasValue(fieldTag, "embedded")) {
			structDiff := diffStruct(fieldVal, prevFiledVal)
			result = append(result, structDiff...)
		} else if fieldVal.Comparable() {
			if fieldVal.Equal(prevFiledVal) {
				continue
			}
			result = append(result, fieldName)
		} else {
			// todo 不能比对的字段先一律更新，待优化比对方案
			result = append(result, fieldName)
		}
	}
	return result
}

// DiffModel 模型差异对比，返回gorm命名规范的更新数据
// 支持以下类型比对：常规数据类型(bool, int, float, string, time.Time)，嵌套可导出的非指针的 struct
func DiffModel(curr, prev interface{}) []string {
	currVal, prevVal := reflect.Indirect(reflect.ValueOf(curr)), reflect.Indirect(reflect.ValueOf(prev))
	if currVal.Kind() != reflect.Struct || prevVal.Kind() != reflect.Struct {
		return nil
	}
	return diffStruct(currVal, prevVal)
}

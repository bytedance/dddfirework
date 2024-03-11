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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gorm.io/datatypes"
)

type TestDiffBasePO struct {
	CreatedAt time.Time
	UpdatedAt time.Time
}

type NestStruct struct {
	Name string
	Num  int64
}

type testDiffPO struct {
	TestDiffBasePO `gorm:"embedded;embeddedPrefix:base_"`

	ID              string
	Name            string
	ItemPrice       int
	DiffPtrValue    *int
	SamePtrValue    *int
	SliceValue      []string
	JSONValue       datatypes.JSON
	SameJSONValue   datatypes.JSON
	StructValue     *NestStruct `gorm:"type:json,serialize:json"`
	SameStructValue *NestStruct `gorm:"type:json,serialize:json"`
	EmptyStruct1    *NestStruct `gorm:"type:json,serialize:json"`
	EmptyStruct2    *NestStruct `gorm:"type:json,serialize:json"`
	EmptyStruct3    *NestStruct `gorm:"type:json,serialize:json"`
}

func TestDiffModel(t *testing.T) {
	i, j := 100, 99
	now := time.Now()
	p1 := testDiffPO{
		TestDiffBasePO: TestDiffBasePO{
			CreatedAt: time.Now(),
			UpdatedAt: now,
		},
		ID:            "p1",
		Name:          "n2",
		ItemPrice:     100,
		DiffPtrValue:  &i,
		SamePtrValue:  &i,
		SliceValue:    []string{"abc"},
		JSONValue:     datatypes.JSON([]byte(`{"a":1}`)),
		SameJSONValue: datatypes.JSON([]byte(`{"a":1}`)),
		StructValue: &NestStruct{
			Name: "s1",
			Num:  1,
		},
		SameStructValue: &NestStruct{
			Name: "s1",
			Num:  2,
		},
		EmptyStruct1: &NestStruct{
			Name: "s1",
			Num:  3,
		},
	}

	p2 := testDiffPO{
		TestDiffBasePO: TestDiffBasePO{
			UpdatedAt: now,
		},
		ID:            "p2",
		Name:          "n2",
		ItemPrice:     50,
		DiffPtrValue:  &j,
		SamePtrValue:  &i,
		SliceValue:    []string{"kkk"},
		JSONValue:     datatypes.JSON([]byte(`{"a":2}`)),
		SameJSONValue: datatypes.JSON([]byte(`{"a":1}`)),
		StructValue: &NestStruct{
			Name: "s2",
			Num:  1,
		},
		SameStructValue: &NestStruct{
			Name: "s1",
			Num:  2,
		},
		EmptyStruct2: &NestStruct{
			Name: "s1",
			Num:  3,
		},
	}

	result := DiffModel(p1, p2)
	assert.ElementsMatch(t, result, []string{"ID", "ItemPrice", "DiffPtrValue", "CreatedAt", "SliceValue", "JSONValue", "StructValue", "UpdatedAt", "EmptyStruct1", "EmptyStruct2"})
}

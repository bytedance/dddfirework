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

package sql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type TestDiffBasePO struct {
	CreatedAt time.Time
	UpdatedAt time.Time
}

type testDiffPO struct {
	TestDiffBasePO `gorm:"embedded;embeddedPrefix:base_"`

	ID           string
	Name         string
	ItemPrice    int
	DiffPtrValue *int
	SamePtrValue *int
	SliceValue   []string
}

func TestDiffModel(t *testing.T) {
	i, j := 100, 99
	now := time.Now()
	p1 := testDiffPO{
		TestDiffBasePO: TestDiffBasePO{
			CreatedAt: time.Now(),
			UpdatedAt: now,
		},
		ID:           "p1",
		Name:         "n2",
		ItemPrice:    100,
		DiffPtrValue: &i,
		SamePtrValue: &i,
		SliceValue:   []string{"abc"},
	}

	p2 := testDiffPO{
		TestDiffBasePO: TestDiffBasePO{
			UpdatedAt: now,
		},
		ID:           "p2",
		Name:         "n2",
		ItemPrice:    50,
		DiffPtrValue: &j,
		SamePtrValue: &i,
		SliceValue:   []string{"kkk"},
	}

	result := DiffModel(p1, p2)
	assert.NotEmpty(t, result)
	assert.Contains(t, result, "ID")
	assert.Contains(t, result, "ItemPrice")
	assert.Contains(t, result, "DiffPtrValue")
	assert.Contains(t, result, "CreatedAt")
	assert.Contains(t, result, "SliceValue")
	assert.NotContains(t, result, "SamePtrValue")
}

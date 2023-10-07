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
	"context"
	"testing"

	"github.com/bytedance/dddfirework/testsuit"
	"github.com/stretchr/testify/assert"
)

type testReturnCommand struct {
	Command
}

func (c *testReturnCommand) Act(ctx context.Context, container RootContainer, roots ...IEntity) error {
	toCreate := &order{
		Title: "test",
	}
	container.Add(toCreate)
	if err := c.Commit(ctx); err != nil {
		return err
	}

	toDel := &order{
		Title: "test2",
	}
	container.Add(toDel)
	if err := c.Commit(ctx); err != nil {
		return err
	}
	container.Remove(toDel)
	if err := c.Commit(ctx); err != nil {
		return err
	}

	c.Output([]string{toCreate.GetID(), toDel.GetID()})
	return nil
}

func TestCommand_Return(t *testing.T) {
	ctx := context.Background()
	db := TestDB{
		Data: map[string]*testModel{},
	}
	locker := testsuit.NewMemLock()
	executor := &MapExecutor{DB: &db}
	res := NewEngine(locker, executor).RunCommand(ctx, &testReturnCommand{})
	assert.NoError(t, res.Error)
	assert.NotEmpty(t, res.Output)

	ids := res.Output.([]string)
	assert.NotEmpty(t, ids[0])
	assert.NotEmpty(t, ids[1])
	assert.Contains(t, db.Data, ids[0])
	assert.NotContains(t, db.Data, ids[1])
}

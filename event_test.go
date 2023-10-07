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

	"github.com/stretchr/testify/assert"
)

type testEvent struct {
	Data string
}

func (t *testEvent) GetType() EventType {
	return "test"
}

func (t *testEvent) GetSender() string {
	return "test"
}

func TestEventRouter(t *testing.T) {
	var getData string
	RegisterEventHandler("test", func(ctx context.Context, evt *testEvent) error {
		getData = evt.Data
		return nil
	})

	ctx := context.Background()
	_ = onEvent(ctx, NewDomainEvent(&testEvent{Data: "helloworld"}))

	assert.Equal(t, "helloworld", getData)
}

func TestDomainEventRouter(t *testing.T) {
	var getData string
	RegisterEventHandler("test", func(ctx context.Context, evt *DomainEvent) error {
		getData = string(evt.Payload)
		return nil
	})

	ctx := context.Background()
	_ = onEvent(ctx, NewDomainEvent(&testEvent{Data: "helloworld"}))

	assert.Contains(t, getData, "helloworld")
}

func TestEventTXChecker(t *testing.T) {
	getData, getData2 := "", ""
	RegisterEventTXChecker("test", func(evt *DomainEvent) TXStatus {
		getData = string(evt.Payload)
		return TXCommit
	})
	RegisterEventTXChecker("test_2", func(evt *DomainEvent) TXStatus {
		return TXCommit
	})

	_ = onTXChecker(NewDomainEvent(&testEvent{Data: "helloworld"}))

	assert.Contains(t, getData, "helloworld")
	assert.Contains(t, getData2, "")
}

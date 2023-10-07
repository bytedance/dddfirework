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

package util

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"

	"github.com/bytedance/dddfirework/example/handler"
)

func Handler(service *handler.SaleServiceImpl) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := context.Background()

		action := r.URL.Query().Get("Action")
		serviceVal := reflect.ValueOf(service)
		method := serviceVal.MethodByName(action)
		if !method.IsValid() {
			http.NotFound(w, r)
			return
		}
		t := method.Type().In(1)
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		req := reflect.New(t)
		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "body invalid", 400)
			return
		}
		if err := json.Unmarshal(data, req.Interface()); err != nil {
			http.Error(w, "body invalid", 400)
			return
		}
		rets := method.Call([]reflect.Value{reflect.ValueOf(ctx), req})
		errValue := rets[1].Interface()
		if errValue != nil {
			http.Error(w, "server error", 500)
			return
		}
		bs, _ := json.Marshal(rets[0].Interface())
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		fmt.Fprint(w, string(bs))
	}
}

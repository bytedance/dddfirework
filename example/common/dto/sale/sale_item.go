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

package sale

type SaleItem struct {
	Code  string `thrift:"Code,1,required" frugal:"1,required,string" json:"Code"`
	Name  string `thrift:"Name,2,required" frugal:"2,required,string" json:"Name"`
	Price int64  `thrift:"Price,3,required" frugal:"3,required,i64" json:"Price"`
	Count int32  `thrift:"Count,4,required" frugal:"4,required,i32" json:"Count"`
}

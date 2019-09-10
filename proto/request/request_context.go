// Copyright 2019 The Vearch Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package request

import "context"

// it use base struct must first field in structs
type RequestContext struct {
    MessageId    string `json:"message_id,omitempty"`
    Timeout      int64  `json:"timeout,omitempty"`
    PartitionNum int    `json:"partition_num,omitempty"`
    RouterAddr   string `json:"router_addr,omitempty"`
    Leader       bool   `json:"leader,omitempty"`
    ctx          context.Context
    store        interface{}
    cannels      []func()
}

func (request *RequestContext) AddCannelFunc(cannel func()) {
    request.cannels = append(request.cannels, cannel)
}

func (request *RequestContext) Cannel() {
    if len(request.cannels) == 0 {
        return
    }
    for _, cannel := range request.cannels {
        cannel()
    }
}

func (request *RequestContext) SetContext(ctx context.Context) {
    request.ctx = ctx
}
func (request *RequestContext) GetContext() context.Context {
    return request.ctx
}

func (request *RequestContext) GetStore() interface{} {
    return request.store
}

func (request *RequestContext) SetStore(store interface{}) {
    request.store = store
}


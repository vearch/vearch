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

package handler

import (
	"runtime/debug"

	"github.com/vearch/vearch/util/log"
)

var DefaultPanicHadler = new(PanicHandler)

type PanicHandler int

//this handler is process ,panic in last of chain
func (PanicHandler) Execute(request *RpcRequest, response *RpcResponse) error {
	if r := recover(); r != nil {
		if response == nil {
			response = new(RpcResponse)
		}
		response.Error = "Server internal error "
		if log.IsInfoEnabled() {
			log.Error("panic error, and the error is :%v", r)
			log.Error(string(debug.Stack()))
		}

		log.Error("execute err : %s", response.Error)
	}
	return nil
}

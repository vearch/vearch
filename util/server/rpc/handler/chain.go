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
	"context"
	"github.com/vearch/vearch/util/log"
)

type ErrorChangeFun func(ctx context.Context, err error, request *RpcRequest, response *RpcResponse) error

func NewChain(name string, paincChain RpcHandler, errChange ErrorChangeFun, handlers ...RpcHandler) *Chain {
	return &Chain{Name: name, panicChain: paincChain, errchange: errChange, chain: handlers}
}

type Chain struct {
	Name       string
	panicChain RpcHandler
	chain      []RpcHandler
	errchange  ErrorChangeFun
}

func (this *Chain) Execute(ctx context.Context, request *RpcRequest, response *RpcResponse) error {
	defer func() {
		if request.Cancel != nil {
			request.Cancel()
		}
		if err := this.panicChain.Execute(request, response); err != nil {
			log.Error("rpc panic err:[%s]", err.Error())
		}
	}()

	request.Ctx = ctx
	for i := 0; i < len(this.chain); i++ {
		if err := this.chain[i].Execute(request, response); err != nil {
			if this.errchange == nil {
				return err
			}
			return this.errchange(ctx, err, request, response)
		}
	}

	return nil
}

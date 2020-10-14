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
	"errors"
	"runtime/debug"

	"github.com/spf13/cast"
	"github.com/vearch/vearch/proto/vearchpb"
	"github.com/vearch/vearch/util/log"
)

type RpcHander interface {
	Execute(ctx context.Context, req *vearchpb.PartitionData, resp *vearchpb.PartitionData) error
}
type ErrorChangeFun func(ctx context.Context, err error, req *vearchpb.PartitionData, reply *vearchpb.PartitionData) error

var DefaultPanicHandler = new(PanicHandler)

type PanicHandler int

func (PanicHandler) Execute(ctx context.Context, req *vearchpb.PartitionData, reply *vearchpb.PartitionData) error {
	if r := recover(); r != nil {
		log.Error("panic error, and the error is :%v", r)
		log.Error(string(debug.Stack()))
		return vearchpb.NewError(vearchpb.ErrorEnum_RECOVER, errors.New(cast.ToString(r)))
	}
	return nil
}
func NewChain(name string, paincChain RpcHander, errChange ErrorChangeFun, handlers ...RpcHander) *Chain {
	return &Chain{Name: name, panicChain: paincChain, errchange: errChange, chain: handlers}
}

type Chain struct {
	Name       string
	panicChain RpcHander
	chain      []RpcHander
	errchange  ErrorChangeFun
}

func (this *Chain) Execute(ctx context.Context, req *vearchpb.PartitionData, reply *vearchpb.PartitionData) error {
	defer func() {
		if err := this.panicChain.Execute(ctx, req, reply); err != nil {
			log.Error("rpc panic err:[%s]", err.Error())
		}
	}()

	for i := 0; i < len(this.chain); i++ {
		if err := this.chain[i].Execute(ctx, req, reply); err != nil {
			if this.errchange == nil {
				return err
			}
			return this.errchange(ctx, err, req, reply)
		}
	}

	return nil
}

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
)

type RpcRequest struct {
	MessageId string             `json:"message_id"`
	Arg       interface{}        `json:"arg"`
	Ctx       context.Context    `json:"-"`
	Cancel    context.CancelFunc `json:"-"`
}

func (this *RpcRequest) SetContext(ctx context.Context) {
	this.Ctx = ctx
}

func (this *RpcRequest) GetContext() context.Context {
	return this.Ctx
}

func (this *RpcRequest) GetMessageID() string {
	return this.MessageId
}
func (this *RpcRequest) GetArg() interface{} {
	return this.Arg
}

type RpcResponse struct {
	MessageId string      `json:"message_id"`
	Error     string      `json:"error"`
	Result    interface{} `json:"result"`
	Status    int64       `json:"status"`
}

func NewRpcResponse(msgId string) *RpcResponse {
	resp := &RpcResponse{}
	resp.MessageId = msgId
	return resp
}

type RpcHandler interface {
	Execute(req *RpcRequest, resp *RpcResponse) error
}

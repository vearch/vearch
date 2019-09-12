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

package server

import (
	"context"
	"github.com/smallnest/rpcx/protocol"
	"github.com/vearch/vearch/util/vearchlog"
	"strings"

	"github.com/smallnest/rpcx/client"
	"github.com/tiglabs/log"
	"github.com/vearch/vearch/util/atomic"
	"github.com/vearch/vearch/util/server/rpc/handler"
)

type RpcClient struct {
	serverAddress []string
	client        *client.OneClient
	used          atomic.AtomicInt64
}

func (this *RpcClient) getClient() *client.OneClient {
	return this.client
}

func (this *RpcClient) GetPoolUsed() int64 {
	return this.used.Get()
}

//client        *client.OneClient
func NewRpcClient(serverAddress ...string) (*RpcClient, error) {

	log.Info("instance client by rpc %s", serverAddress[0])
	var d client.ServiceDiscovery
	if len(serverAddress) == 1 {
		d = client.NewPeer2PeerDiscovery("tcp@"+serverAddress[0], "")
	} else {
		arr := make([]*client.KVPair, len(serverAddress))

		for i, addr := range serverAddress {
			arr[i] = &client.KVPair{Key: addr}
		}

		d = client.NewMultipleServersDiscovery(arr)
	}
	one := client.NewOneClient(client.Failtry, client.RandomSelect, d, client.DefaultOption)

	return &RpcClient{serverAddress: serverAddress, client: one}, nil
}

func (this *RpcClient) Close() error {
	return this.client.Close()
}

func (this *RpcClient) Execute(servicePath string, req *handler.RpcRequest) (*handler.RpcResponse, error) {

	resp := handler.NewRpcResponse(req.MessageId)

	oneClient := this.getClient()
	this.used.Incr()

	if err := oneClient.Call(req.Ctx, servicePath, serviceMethod, req, resp); err != nil {
		return nil, err
	}

	return resp, nil
}

//execute rpc method , with whie callback
func (this *RpcClient) GoExecute(servicePath string, req *handler.RpcRequest) (*client.Call, error) {
	resp := handler.NewRpcResponse(req.MessageId)
	oneClient := this.getClient()
	return oneClient.Go(req.Ctx, servicePath, serviceMethod, req, resp, nil)
}

type StreamCallback func(msg *protocol.Message) error

func (this *RpcClient) StreamExecute(ctx context.Context, servicePath string, req *handler.RpcRequest, sc StreamCallback) (*handler.RpcResponse, error) {
	log.Info("to instance bidirect xclient by addr:[%s]", this.GetAddress(0))
	d := client.NewPeer2PeerDiscovery("tcp@"+this.GetAddress(0), "")
	defer d.Close()
	ch := make(chan *protocol.Message, 100)
	defer close(ch)
	xclient := client.NewBidirectionalXClient(servicePath, client.Failtry, client.RandomSelect, d, client.DefaultOption, ch)
	defer vearchlog.CloseIfNotNil(xclient)
	resp := handler.NewRpcResponse(req.MessageId)
	go func() {
		if err := xclient.Call(ctx, serviceMethod, req, resp); err != nil {
			return
		}
	}()

	for {
		select {
		case msg, ok := <-ch:
			if !ok || len(msg.Payload) == 0 {
				return resp, nil
			}
			if err := sc(msg); err != nil {
				return nil, err
			}
		case <-ctx.Done():
			return resp, ctx.Err()
		}
	}
}

func (this *RpcClient) GetAddress(i int) string {
	if this == nil || len(this.serverAddress) <= i {
		return ""
	}
	if i < 0 {
		return strings.Join(this.serverAddress, ",")
	}
	return this.serverAddress[i]
}

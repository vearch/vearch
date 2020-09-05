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
	"strings"

	"github.com/vearch/vearch/proto/vearchpb"

	"github.com/smallnest/pool"

	"github.com/smallnest/rpcx/client"
	"github.com/vearch/vearch/util/log"
)

type RpcClient struct {
	serverAddress []string
	clientPool    *pool.Pool
}

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

	clientPool := &pool.Pool{New: func() interface{} {
		log.Info("to instance client for server:[%s]", serverAddress)
		return client.NewOneClient(client.Failtry, client.RandomSelect, d, ClientOption)
	}}

	return &RpcClient{serverAddress: serverAddress, clientPool: clientPool}, nil
}

func (this *RpcClient) Close() error {
	var e error
	this.clientPool.Range(func(v interface{}) bool {
		if err := v.(*client.OneClient).Close(); err != nil {
			log.Error("close client has err:[%s]", err.Error())
			e = err
		}
		return true
	})
	return e
}

func (this *RpcClient) Execute(ctx context.Context, servicePath string, args interface{}, reply *vearchpb.PartitionData) (err error) {
	cli := this.clientPool.Get().(*client.OneClient)
	defer this.clientPool.Put(cli)
	if err := cli.Call(ctx, servicePath, serviceMethod, args, reply); err != nil {
		return vearchpb.NewError(vearchpb.ErrorEnum_Call_RpcClient_Failed, err)
	}
	return nil
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

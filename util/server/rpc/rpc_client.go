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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/config"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/proto/vearchpb"

	"github.com/smallnest/pool"

	"github.com/smallnest/rpcx/client"
	"github.com/smallnest/rpcx/share"
	"github.com/vearch/vearch/util/log"
)

var defaultConcurrentNum int = 2000

type RpcClient struct {
	serverAddress []string
	clientPool    *pool.Pool
	concurrent    chan bool
	concurrentNum int
}

func NewRpcClient(serverAddress ...string) (*RpcClient, error) {
	log.Debug("instance client by rpc %s", serverAddress[0])
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
		log.Debug("to instance client for server:[%s]", serverAddress)
		oneclient := client.NewOneClient(client.Failtry, client.RandomSelect, d, ClientOption)
		return oneclient
	}}

	r := &RpcClient{serverAddress: serverAddress, clientPool: clientPool}
	r.concurrentNum = defaultConcurrentNum
	if config.Conf().Router.ConcurrentNum > 0 {
		r.concurrentNum = config.Conf().Router.ConcurrentNum
	}
	r.concurrent = make(chan bool, r.concurrentNum)
	return r, nil
}

func (r *RpcClient) Close() error {
	var e error
	r.clientPool.Range(func(v interface{}) bool {
		if err := v.(*client.OneClient).Close(); err != nil {
			log.Error("close client has err:[%s]", err.Error())
			e = err
		}
		return true
	})
	return e
}

func (r *RpcClient) Execute(ctx context.Context, servicePath string, args interface{}, reply *vearchpb.PartitionData) (err error) {
	r.concurrent <- true
	defer func() {
		<-r.concurrent
		if r := recover(); r != nil {
			err = errors.New(cast.ToString(r))
			log.Error(err.Error())
		}
	}()
	select {
	case <-ctx.Done():
		msg := fmt.Sprintf("Too much concurrency causes time out, the max num of concurrency is [%d]", r.concurrentNum)
		err = vearchpb.NewError(vearchpb.ErrorEnum_TIMEOUT, errors.New(msg))
		return
	default:
		var (
			md map[string]string
			ok bool
		)
		if m := ctx.Value(share.ReqMetaDataKey); m != nil {
			md, ok = m.(map[string]string)
			if !ok {
				md = make(map[string]string)
			}
		} else {
			md = make(map[string]string)
		}
		if endTime, ok := ctx.Value(entity.RPC_TIME_OUT).(time.Time); ok {
			timeout := int64(time.Until(endTime) / time.Millisecond)
			if timeout < 1 {
				messageID := ctx.Value(entity.MessageID).(string)
				msg := fmt.Sprintf("messageID[%s]: timeout when execute rpc, the max num of concurrency is [%d]", messageID, r.concurrentNum)
				err = vearchpb.NewError(vearchpb.ErrorEnum_TIMEOUT, errors.New("timeout"))
				log.Errorf(msg)
				return
			}
			md[string(entity.RPC_TIME_OUT)] = strconv.FormatInt(int64(timeout), 10)
		}
		if span := opentracing.SpanFromContext(ctx); span != nil {
			span.Tracer().Inject(span.Context(), opentracing.TextMap, opentracing.TextMapCarrier(md))
		}
		ctx = context.WithValue(ctx, share.ReqMetaDataKey, md)
		cli := r.clientPool.Get().(*client.OneClient)
		defer r.clientPool.Put(cli)
		if err := cli.Call(ctx, servicePath, serviceMethod, args, reply); err != nil {
			err = vearchpb.NewError(vearchpb.ErrorEnum_Call_RpcClient_Failed, err)
		}
		return
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

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
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/smallnest/rpcx/client"
	"github.com/smallnest/rpcx/protocol"
	"github.com/smallnest/rpcx/server"
	"github.com/smallnest/rpcx/share"
	"github.com/vearch/vearch/util/server/rpc/handler"
)

//var serializeType = protocol.MsgPack
var serializeType = protocol.ProtoBuffer

const serviceMethod = "Execute"

var defaultCodec = &PBCodec{}

func init() {
	share.RegisterCodec(serializeType, defaultCodec)
	client.DefaultOption.SerializeType = serializeType
}

type RpcServer struct {
	serverAddress string
	port          uint16
	server        *server.Server
}

func NewRpcServer(ip string, port uint16) *RpcServer {
	if port == 0 {
		panic(errors.New("can not found module.rpc-port in config"))
	}
	return &RpcServer{serverAddress: ip, port: port}
}

func (this *RpcServer) Run() error {
	if strings.Compare(this.serverAddress, "127.0.0.1") == 0 || strings.Compare(this.serverAddress, "localhost") == 0 {
		this.serverAddress = ""
	}
	this.server = server.NewServer()
	this.server.Plugins.Add(client.OpenTracingPlugin{})
	go this.server.Serve("tcp", fmt.Sprintf("%s:%d", this.serverAddress, this.port))

	return nil
}

func (this RpcServer) Stop() {
	if this.server != nil {
		this.server.Close()
	}
}

func (this RpcServer) ActiveClientConn() []net.Conn {
	return this.server.ActiveClientConn()
}

func (this *RpcServer) RegisterName(chain *handler.Chain, meta string) error {
	return this.server.RegisterName(chain.Name, chain, meta)
}

func (this *RpcServer) RegisterHandler(name string, rcvr interface{}, meta string) error {
	return this.server.RegisterName(name, rcvr, meta)
}

//send mesage by bidirectional
func (this *RpcServer) SendMessage(conn net.Conn, servicePath string, data []byte) error {
	return this.server.SendMessage(conn, servicePath, serviceMethod, nil, data)
}

func (this *RpcServer) AddPlugin(plugin server.Plugin) {
	this.server.Plugins.Add(plugin)
}

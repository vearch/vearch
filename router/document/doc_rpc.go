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

package document

import (
	"context"
	"fmt"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/client"
	"github.com/vearch/vearch/config"
	pkg "github.com/vearch/vearch/proto"
	"github.com/vearch/vearch/proto/pspb"
	"github.com/vearch/vearch/proto/request"
	"github.com/vearch/vearch/util"
	"github.com/vearch/vearch/util/cbjson"
	"github.com/vearch/vearch/util/log"
	"github.com/vearch/vearch/util/uuid"
	"google.golang.org/grpc"
	"time"
)

type RpcHandler struct {
	rpcServer  *grpc.Server
	docService docService
	client     *client.Client
}

func ExportRpcHandler(rpcServer *grpc.Server, client *client.Client) {
	docService := newDocService(client)

	rpcHandler := &RpcHandler{
		rpcServer:  rpcServer,
		docService: *docService,
		client:     client,
	}

	pspb.RegisterRpcApiServer(rpcServer, rpcHandler)
}

func (handler *RpcHandler) Search(ctx context.Context, req *pspb.RpcSearchRequest) (def *pspb.RpcSearchResponse, err error) {
	def = &pspb.RpcSearchResponse{Head: handler.newErrHead(pkg.CodeErr(pkg.ERRCODE_INTERNAL_ERROR))}

	if req == nil || req.Head == nil {
		return nil, fmt.Errorf("request or head is nil ")
	}

	defer func() {
		if r := recover(); r != nil {
			log.Error("panic err!!! err:[%s]", cast.ToString(r))
		}
	}()
	ctx, cancel := handler.setTimeout(ctx, req)
	defer func() {
		if cancel != nil {
			cancel()
		}
	}()

	if err := handler.validateAuth(ctx, req); err != nil {
		return &pspb.RpcSearchResponse{Head: handler.newErrHead(err)}, nil
	}

	searchRequest := request.NewSearchRequest(ctx, uuid.FlakeUUID())
	if len(req.Query) != 0 {
		err := cbjson.Unmarshal([]byte(req.Query), searchRequest.SearchDocumentRequest)
		if err != nil {
			return &pspb.RpcSearchResponse{Head: handler.newErrHead(err)}, nil
		}
	}

	if searchRequest.Size == nil {
		searchRequest.Size = util.PInt(20)
	}

	var clientType client.ClientType

	switch req.ClientType {
	case "leader", "":
		clientType = client.LEADER
	case "random":
		clientType = client.RANDOM
	default:
		return &pspb.RpcSearchResponse{Head: handler.newErrHead(fmt.Errorf("client_type err param:[%s] , it use `leader` or `random`", req.ClientType))}, nil
	}

	t1 := time.Now()
	searchResponse, nameCache, err := handler.docService.searchDoc(ctx, req.DbName, req.SpaceName, searchRequest, clientType)
	if err != nil {
		return &pspb.RpcSearchResponse{Head: handler.newErrHead(err)}, nil
	}
	t2 := time.Now()

	idIsLong := false
	space, err := handler.docService.getSpace(ctx, req.DbName, req.SpaceName)
	if err != nil {
		return &pspb.RpcSearchResponse{Head: handler.newErrHead(err)}, nil
	} else {
		idType := space.Engine.IdType
		if idType != "" && ("long" == idType || "Long" == idType) {
			idIsLong = true
		}
	}

	bs, err := searchResponse.ToContent(searchRequest.From, *searchRequest.Size, nameCache, false, t2.Sub(t1), idIsLong)
	if err != nil {
		return &pspb.RpcSearchResponse{Head: handler.newErrHead(err)}, nil
	}

	return &pspb.RpcSearchResponse{Head: handler.newOkHead(), Body: string(bs)}, nil
}

//--------------------- util tools ---------------------//

func (rh *RpcHandler) setTimeout(ctx context.Context, request *pspb.RpcSearchRequest) (context.Context, context.CancelFunc) {
	if request.Head.TimeOutMs > 0 {
		return context.WithTimeout(ctx, time.Duration(request.Head.TimeOutMs)*time.Millisecond)
	}
	return ctx, nil
}

func (rh *RpcHandler) validateAuth(ctx context.Context, request *pspb.RpcSearchRequest) error {
	if config.Conf().Global.SkipAuth {
		return nil
	}

	user, _ := rh.client.Master().Cache().UserByCache(ctx, request.Head.UserName)
	if user == nil {
		log.Error("user not found , name:[%s]  ", request.Head.UserName)
		return pkg.CodeErr(pkg.ERRCODE_NAME_OR_PASSWORD)
	}
	if user.Password != request.Head.Password {
		log.Error("auth password not matched. ")
		return pkg.CodeErr(pkg.ERRCODE_NAME_OR_PASSWORD)
	}

	return nil
}

func (rh *RpcHandler) newErrHead(err error) *pspb.RpcResponseHead {
	if err == nil {
		err = fmt.Errorf("not err error")
	}
	return &pspb.RpcResponseHead{Error: err.Error(), Code: pkg.ErrCode(err)}
}

func (rh *RpcHandler) newOkHead() *pspb.RpcResponseHead {
	return &pspb.RpcResponseHead{Code: pkg.ERRCODE_SUCCESS}
}

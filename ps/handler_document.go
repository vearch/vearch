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

package ps

import (
	"context"
	"encoding/json"
	"github.com/smallnest/rpcx/server"
	"github.com/tiglabs/log"
	"github.com/vearch/vearch/client"
	"github.com/vearch/vearch/config"
	"github.com/vearch/vearch/proto"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/proto/pspb"
	"github.com/vearch/vearch/proto/request"
	"github.com/vearch/vearch/proto/response"
	"github.com/vearch/vearch/ps/engine"
	"github.com/vearch/vearch/util/cbjson"
	rpc "github.com/vearch/vearch/util/server/rpc"
	"github.com/vearch/vearch/util/server/rpc/handler"
	"go.uber.org/atomic"
	"net"
	"strings"
	"time"
)

func ExportToRpcHandler(server *Server) {

	initHandler := &InitHandler{server: server}
	frozenHandler := &FrozenHandler{server: server}
	psErrorChange := psErrorChange(server)
	streamSearchHandler := &StreamSearchHandler{server: server.rpcServer}

	limitPlugin := &limitPlugin{limit: atomic.NewInt64(0), size: 50000}

	server.rpcServer.AddPlugin(limitPlugin)

	//register search handler
	if err := server.rpcServer.RegisterName(handler.NewChain(client.SearchHandler, server.monitor, handler.DefaultPanicHadler, psErrorChange, initHandler, new(SearchHandler)), ""); err != nil {
		panic(err)
	}

	if err := server.rpcServer.RegisterName(handler.NewChain(client.DeleteByQueryHandler, server.monitor, handler.DefaultPanicHadler, psErrorChange, initHandler, new(DeleteByQueryHandler)), ""); err != nil {
		panic(err)
	}

	if err := server.rpcServer.RegisterName(handler.NewChain(client.MSearchHandler, server.monitor, handler.DefaultPanicHadler, psErrorChange, initHandler, new(MSearchHandler)), ""); err != nil {
		panic(err)
	}

	if err := server.rpcServer.RegisterName(handler.NewChain(client.StreamSearchHandler, server.monitor, handler.DefaultPanicHadler, psErrorChange, initHandler, streamSearchHandler), ""); err != nil {
		panic(err)
	}

	if err := server.rpcServer.RegisterName(handler.NewChain(client.GetDocHandler, server.monitor, handler.DefaultPanicHadler, psErrorChange, initHandler, new(GetDocHandler)), ""); err != nil {
		panic(err)
	}

	if err := server.rpcServer.RegisterName(handler.NewChain(client.GetDocsHandler, server.monitor, handler.DefaultPanicHadler, psErrorChange, initHandler, new(GetDocsHandler)), ""); err != nil {
		panic(err)
	}

	if err := server.rpcServer.RegisterName(handler.NewChain(client.BatchHandler, server.monitor, handler.DefaultPanicHadler, psErrorChange, initHandler, frozenHandler, &BatchHandler{server: server}), ""); err != nil {
		panic(err)
	}

	if err := server.rpcServer.RegisterName(handler.NewChain(client.WriteHandler, server.monitor, handler.DefaultPanicHadler, psErrorChange, initHandler, frozenHandler, &WriteHandler{server: server, limitPlugin: limitPlugin}), ""); err != nil {
		panic(err)
	}

	if err := server.rpcServer.RegisterName(handler.NewChain(client.FlushHandler, server.monitor, handler.DefaultPanicHadler, psErrorChange, initHandler, &FlushHandler{server: server}), ""); err != nil {
		panic(err)
	}

	if err := server.rpcServer.RegisterName(handler.NewChain(client.ForceMergeHandler, server.monitor, handler.DefaultPanicHadler, psErrorChange, initHandler, &ForceMergeHandler{server: server}), ""); err != nil {
		panic(err)
	}

}

//add context and set timeout if timeout > 0, add store engine , limit request doc num
type InitHandler struct {
	server *Server
}

func (i *InitHandler) Execute(req *handler.RpcRequest, resp *handler.RpcResponse) error {
	if i.server.stopping.Get() {
		return pkg.ErrGeneralServiceUnavailable
	}

	arg := req.Arg.(request.Request)
	rCtx := arg.Context()
	if rCtx.Timeout > 0 {
		ctx, cancel := context.WithTimeout(req.Ctx, time.Duration(rCtx.Timeout)*time.Second)
		arg.SetContext(ctx)
		req.Cancel = cancel
	} else {
		arg.SetContext(req.Ctx)
	}

	if store := i.server.GetPartition(arg.GetPartitionID()); store == nil {
		log.Error("partition not found, partitionId:[%d]", arg.GetPartitionID())
		return pkg.ErrPartitionNotExist
	} else {
		rCtx.SetStore(store)
	}

	return nil
}

type FrozenHandler struct {
	server *Server
}

func (i *FrozenHandler) Execute(req *handler.RpcRequest, resp *handler.RpcResponse) error {
	reqs := req.Arg.(request.Request)
	rCtx := reqs.Context()
	store := rCtx.GetStore().(PartitionStore)
	if store.GetPartition().Frozen {
		return pkg.ErrPartitionFrozen
	}
	return nil
}

//create update index handler
type BatchHandler struct {
	server *Server
}

func (wh *BatchHandler) Execute(req *handler.RpcRequest, resp *handler.RpcResponse) error {

	reqs := req.GetArg().(*request.ObjRequest)
	docs := make([]*pspb.DocCmd, 0, 10)
	if err := reqs.Decode(&docs); err != nil {
		return err
	}

	store := reqs.GetStore().(PartitionStore)

	for _, doc := range docs {
		if doc.Type != pspb.OpType_DELETE && doc.Type != pspb.OpType_NOOP {
			if err := fullFieldAndUpdateSchema(wh.server, req.Ctx, store.GetEngine(), store.GetSpace(), doc); err != nil {
				return err
			}
		}
	}

	result := make(response.WriteResponse, len(docs))
	for i := 0; i < len(docs); i++ {
		if reps, err := store.Write(req.Ctx, docs[i]); err != nil {
			log.Error("insert doc err :[%s] , content :[%s]", err.Error(), docs[i].Source)
			result[i] = response.NewErrDocResult(docs[i].DocId, err)
		} else {
			result[i] = reps
		}
	}

	resp.Result = result

	return nil
}

//create update index handler
type WriteHandler struct {
	server      *Server
	limitPlugin *limitPlugin
}

type limitPlugin struct {
	size  int64
	limit *atomic.Int64
}

func (lp *limitPlugin) HandleConnAccept(conn net.Conn) (net.Conn, bool) {
	if lp.limit.Load() > lp.size {

		for _, m := range config.Conf().Masters {
			if m.Address == conn.RemoteAddr().Network() || strings.HasPrefix(conn.RemoteAddr().Network(), m.Address+":") {
				log.Info("too many routine:[%d]  but this conn is master so can conn")
				return conn, true
			}
		}

		log.Warn("too many routine:[%d] for limt so skip %s conn", lp.limit.Load(), conn.RemoteAddr().String())
		return conn, false
	}
	return conn, true
}

func (wh *WriteHandler) Execute(req *handler.RpcRequest, resp *handler.RpcResponse) error {

	wh.limitPlugin.limit.Inc()
	defer wh.limitPlugin.limit.Dec()

	if wh.limitPlugin.limit.Load() > wh.limitPlugin.size {
		log.Warn("too many routine:[%d] for limt so skip pre read request", wh.limitPlugin.limit.Load())
		return pkg.ErrGeneralSysBusy
	}

	reqs := req.GetArg().(*request.ObjRequest)
	doc := pspb.GetDocCmd()
	defer func() {
		pspb.PutDocCmd(doc)
	}()
	if err := reqs.Decode(doc); err != nil {
		return err
	}

	store := reqs.GetStore().(PartitionStore)

	if doc.Type != pspb.OpType_DELETE && doc.Type != pspb.OpType_NOOP {
		if err := fullFieldAndUpdateSchema(wh.server, req.Ctx, store.GetEngine(), store.GetSpace(), doc); err != nil {
			return err
		}
	}

	if reps, err := store.Write(req.Ctx, doc); err != nil {
		log.Error("handler type: [%s] doc err :[%s] , content :[%s]", doc.Type, err.Error(), doc.Source)
		return err
	} else {
		resp.Result = reps
	}

	return nil
}

//flush index handler
type FlushHandler struct {
	server *Server
}

func (wh *FlushHandler) Execute(req *handler.RpcRequest, resp *handler.RpcResponse) error {
	reqs := req.Arg.(request.Request)
	store := reqs.Context().GetStore().(PartitionStore)
	err := store.Flush(req.Ctx)
	if err != nil {
		return err
	}

	return nil
}

//forceMerge index handler
type ForceMergeHandler struct {
	server *Server
}

func (wh *ForceMergeHandler) Execute(req *handler.RpcRequest, resp *handler.RpcResponse) error {
	reqs := req.Arg.(request.Request)
	store := reqs.Context().GetStore().(PartitionStore)
	err := store.GetEngine().Optimize()
	if err != nil {
		return err
	}

	return nil
}

func fullFieldAndUpdateSchema(server *Server, ctx context.Context, engine engine.Engine, space entity.Space, doc *pspb.DocCmd) error {

	fields, dynamicFieldMap, err := engine.MapDocument(doc)
	if err != nil {
		return err
	}
	doc.Fields = fields
	if len(dynamicFieldMap) > 0 {

		newMapping := make(map[string]interface{})

		for path, fieldType := range dynamicFieldMap {
			split := strings.Split(path, ".")
			var temp interface{}
			pre := newMapping
			for i := 0; i < len(split)-1; i++ {
				temp = pre[split[i]]
				if temp == nil {
					pre[split[i]] = map[string]interface{}{"properties": make(map[string]interface{})}
					temp = pre[split[i]]
				} else {
					temp = temp.(map[string]interface{})
				}
				pre = temp.(map[string]interface{})["properties"].(map[string]interface{})
			}
			pre[split[len(split)-1]] = map[string]interface{}{"type": strings.ToLower(fieldType.String())}
		}

		bytes, err := json.Marshal(newMapping)
		if err != nil {
			return err
		}

		dbName, err := server.client.Master().QueryDBId2Name(ctx, space.DBId)
		if err != nil {
			return err
		}

		newSpace := &entity.Space{Id: space.Id, Name: space.Name, Properties: bytes}

		if err := server.client.Master().UpdateSpace(ctx, dbName, newSpace); err != nil {
			return err
		}
	}
	return nil
}

//retrieve handler
type GetDocHandler int

func (*GetDocHandler) Execute(req *handler.RpcRequest, resp *handler.RpcResponse) error {
	reqs := req.GetArg().(*request.ObjRequest)
	docID := ""
	if err := reqs.Decode(&docID); err != nil {
		return err
	}
	if reps, err := reqs.GetStore().(PartitionStore).GetDocument(req.Ctx, reqs.Leader, docID); err != nil {
		return err
	} else {
		resp.Result = reps
	}
	return nil
}

//retrieve handler
type GetDocsHandler int

func (*GetDocsHandler) Execute(req *handler.RpcRequest, resp *handler.RpcResponse) error {
	reqs := req.GetArg().(*request.ObjRequest)
	ids := make([]string, 0, 10)
	if err := reqs.Decode(&ids); err != nil {
		return err
	}

	if responses, err := reqs.GetStore().(PartitionStore).GetDocuments(req.Ctx, reqs.Leader, ids); err != nil {
		return err
	} else {
		resp.Result = responses
	}
	return nil

}

//deleteByQuery handler
type DeleteByQueryHandler int

func (*DeleteByQueryHandler) Execute(req *handler.RpcRequest, resp *handler.RpcResponse) error {
	reqs := req.GetArg().(*request.SearchRequest)
	if reqs.SearchDocumentRequest == nil {
		reqs.SearchDocumentRequest = &request.SearchDocumentRequest{}
	}

	delCount, err := reqs.GetStore().(PartitionStore).DeleteByQuery(req.Ctx, reqs.Leader, reqs)
	if err != nil {
		return err
	}

	resp.Result = delCount

	return nil
}

//search handler
type SearchHandler int

func (*SearchHandler) Execute(req *handler.RpcRequest, resp *handler.RpcResponse) error {
	reqs := req.GetArg().(*request.SearchRequest)
	if reqs.SearchDocumentRequest == nil {
		reqs.SearchDocumentRequest = &request.SearchDocumentRequest{}
	}

	if reps, e := reqs.GetStore().(PartitionStore).Search(req.Ctx, reqs.Leader, reqs); e != nil {
		return e
	} else {
		resp.Result = reps
	}

	return nil
}

//Msearch handler
type MSearchHandler int

func (*MSearchHandler) Execute(req *handler.RpcRequest, resp *handler.RpcResponse) error {
	reqs := req.GetArg().(*request.SearchRequest)
	if reqs.SearchDocumentRequest == nil {
		reqs.SearchDocumentRequest = &request.SearchDocumentRequest{}
	}

	if reps, e := reqs.GetStore().(PartitionStore).MSearch(req.Ctx, reqs.Leader, reqs); e != nil {
		return e
	} else {
		resp.Result = reps
	}

	return nil
}

//search handler
type StreamSearchHandler struct {
	server *rpc.RpcServer
}

func (ssh *StreamSearchHandler) Execute(req *handler.RpcRequest, resp *handler.RpcResponse) error {
	reqs := req.GetArg().(*request.SearchRequest)
	if reqs.SearchDocumentRequest == nil {
		reqs.SearchDocumentRequest = &request.SearchDocumentRequest{}
	}

	resultChan := make(chan *response.DocResult, 100)

	go func() {
		_ = reqs.GetStore().(PartitionStore).StreamSearch(req.Ctx, reqs.Leader, reqs, resultChan)
	}()

	conn := req.Ctx.Value(server.RemoteConnContextKey).(net.Conn)
	if conn == nil {
		return pkg.ErrGeneralServiceUnavailable
	}

	defer func() {
		if err := ssh.server.SendMessage(conn, client.StreamSearchHandler, nil); err != nil {
			log.Error(err.Error())
		}
	}()
	for {
		select {
		case result, ok := <-resultChan:
			if !ok {
				return nil
			}

			bytes, err := cbjson.Marshal(result)
			if err != nil {
				return err
			}

			err = ssh.server.SendMessage(conn, client.StreamSearchHandler, bytes)
			if err != nil {
				return err
			}
		case <-req.Ctx.Done():
			return req.Ctx.Err()
		}
	}

}

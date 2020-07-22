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
	"bytes"
	"context"
	"fmt"
	"github.com/vearch/vearch/monitor"
	"html/template"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/vearch/vearch/proto/request"
	"github.com/vearch/vearch/proto/response"
	"github.com/vearch/vearch/router/document/resp"
	"github.com/vearch/vearch/util"
	"github.com/vearch/vearch/util/cbjson"

	pkg "github.com/vearch/vearch/proto"
	"github.com/vearch/vearch/util/uuid"

	"github.com/spf13/cast"
	"github.com/vearch/vearch/client"
	"github.com/vearch/vearch/config"
	"github.com/vearch/vearch/util/log"
	"github.com/vearch/vearch/util/netutil"
	"github.com/vearch/vearch/util/regularutil"
)

const (
	UrlQueryFrom            = "from"
	UrlQuerySize            = "size"
	UrlQueryRouting         = "routing"
	UrlQueryTypedKey        = "typed_keys"
	UrlQueryVersion         = "version"
	UrlQueryRetryOnConflict = "retry_on_conflict"
	UrlQueryOpType          = "op_type"
	UrlQueryRefresh         = "refresh"
	UrlQueryURISort         = "sort"
	UrlQueryTimeout         = "timeout"
	ClientTypeValue         = "client_type"
)

const (
	headerAuthKey = "Authorization"
)

const (
	UrlParamDbName    = "db_name"
	UrlParamSpaceName = "space_name"
	UrlParamDocID     = "doc_id"
)

type RawReqBody []byte

type RawReqArgs map[string]string

var doMappingSuccess = []byte(`{"acknowledged":true}`)

type DocumentHandler struct {
	httpServer *netutil.Server
	docService docService
	client     *client.Client
}

func ExportDocumentHandler(httpServer *netutil.Server, client *client.Client) {
	docService := newDocService(client)

	documentHandler := &DocumentHandler{
		httpServer: httpServer,
		docService: *docService,
		client:     client,
	}
	if err := documentHandler.ExportToServer(); err != nil {
		panic(err)
	}

}

func (handler *DocumentHandler) ExportToServer() error {
	// cluster info: /
	handler.httpServer.HandlesMethods([]string{http.MethodGet}, fmt.Sprintf("/"), []netutil.HandleContinued{handler.handleAuth, handler.handleClusterInfo}, nil)

	// bulk: /$dbName/$spaceName/_bulk
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, fmt.Sprintf("/{%s}/{%s}/_bulk", UrlParamDbName, UrlParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleBulk}, nil)
	// bulk: /$dbName/_bulk
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, fmt.Sprintf("/{%s}/_bulk", UrlParamDbName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleBulk}, nil)
	// bulk: /_bulk
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, fmt.Sprintf("/_bulk"), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleBulk}, nil)

	// get space mapping: /$dbName/_mapping/$spaceName
	handler.httpServer.HandlesMethods([]string{http.MethodGet}, fmt.Sprintf("/{%s}/_mapping/{%s}", UrlParamDbName, UrlParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleGetSpaceMapping}, nil)
	// get space mapping: /$dbName/_mapping/$spaceName/
	handler.httpServer.HandlesMethods([]string{http.MethodGet}, fmt.Sprintf("/{%s}/_mapping/{%s}/", UrlParamDbName, UrlParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleGetSpaceMapping}, nil)

	// flush space: /$dbName/$spaceName/_flush
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, fmt.Sprintf("/{%s}/{%s}/_flush", UrlParamDbName, UrlParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleFlush}, nil)

	// forcemerge space: /$dbName/$spaceName/_forcemerge
	handler.httpServer.HandlesMethods([]string{http.MethodPost}, fmt.Sprintf("/{%s}/{%s}/_forcemerge", UrlParamDbName, UrlParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleForceMerge}, nil)

	// create doc: /$dbName/$spaceName/$docId/_create
	handler.httpServer.HandlesMethods([]string{http.MethodPut}, fmt.Sprintf("/{%s}/{%s}/{%s}/_create", UrlParamDbName, UrlParamSpaceName, UrlParamDocID), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleCreateDoc}, nil)

	// update doc: /$dbName/$spaceName/$docId/_update
	handler.httpServer.HandlesMethods([]string{http.MethodPost, http.MethodPut}, fmt.Sprintf("/{%s}/{%s}/{%s}/_update", UrlParamDbName, UrlParamSpaceName, UrlParamDocID), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleUpdateDoc}, nil)

	// search doc: /$dbName/$spaceName/_msearch
	handler.httpServer.HandlesMethods([]string{http.MethodGet, http.MethodPost}, fmt.Sprintf("/{%s}/{%s}/_msearch", UrlParamDbName, UrlParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleMSearchDoc}, nil)

	// search doc: /$dbName/$spaceName/_msearch_new
	handler.httpServer.HandlesMethods([]string{http.MethodGet, http.MethodPost}, fmt.Sprintf("/{%s}/{%s}/_msearch_new", UrlParamDbName, UrlParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleMSearchNewDoc}, nil)

	// search doc: /$dbName/$spaceName/_msearch_ids
	handler.httpServer.HandlesMethods([]string{http.MethodGet, http.MethodPost}, fmt.Sprintf("/{%s}/{%s}/_msearch_ids", UrlParamDbName, UrlParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleMSearchIDsDoc}, nil)

	// search doc: /$dbName/$spaceName/_msearch_forids
	handler.httpServer.HandlesMethods([]string{http.MethodGet, http.MethodPost}, fmt.Sprintf("/{%s}/{%s}/_msearch_forids", UrlParamDbName, UrlParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleMSearchForIDsDoc}, nil)

	// search doc: /$dbName/$spaceName/_search
	handler.httpServer.HandlesMethods([]string{http.MethodGet, http.MethodPost}, fmt.Sprintf("/{%s}/{%s}/_search", UrlParamDbName, UrlParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleSearchDoc}, nil)

	handler.httpServer.HandlesMethods([]string{http.MethodDelete, http.MethodPost}, fmt.Sprintf("/{%s}/{%s}/_delete_by_query", UrlParamDbName, UrlParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleDeleteByQuery}, nil)

	handler.httpServer.HandlesMethods([]string{http.MethodGet, http.MethodPost}, fmt.Sprintf("/{%s}/{%s}/_stream_search", UrlParamDbName, UrlParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleStreamSearchDoc}, nil)

	// replace doc: /$dbName/$spaceName
	handler.httpServer.HandlesMethods([]string{http.MethodPost, http.MethodPut}, fmt.Sprintf("/{%s}/{%s}", UrlParamDbName, UrlParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleReplaceDoc}, nil)
	// replace doc: /$dbName/$spaceName/
	handler.httpServer.HandlesMethods([]string{http.MethodPost, http.MethodPut}, fmt.Sprintf("/{%s}/{%s}/", UrlParamDbName, UrlParamSpaceName), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleReplaceDoc}, nil)
	// replace doc: /$dbName/$spaceName/$docId
	handler.httpServer.HandlesMethods([]string{http.MethodPost, http.MethodPut}, fmt.Sprintf("/{%s}/{%s}/{%s}", UrlParamDbName, UrlParamSpaceName, UrlParamDocID), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleReplaceDoc}, nil)

	// delete doc: /$dbName/$spaceName/$docId
	handler.httpServer.HandlesMethods([]string{http.MethodDelete}, fmt.Sprintf("/{%s}/{%s}/{%s}", UrlParamDbName, UrlParamSpaceName, UrlParamDocID), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleDeleteDoc}, nil)

	// get doc: /$dbName/$spaceName/$docId
	handler.httpServer.HandlesMethods([]string{http.MethodGet}, fmt.Sprintf("/{%s}/{%s}/{%s}", UrlParamDbName, UrlParamSpaceName, UrlParamDocID), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.handleGetDoc}, nil)

	// bulk: /basic_auth
	handler.httpServer.HandlesMethods([]string{http.MethodPost, http.MethodGet}, fmt.Sprintf("/_encrypt"), []netutil.HandleContinued{handler.handleTimeout, handler.namePasswordEncrypt}, nil)

	//get cache info
	handler.httpServer.HandlesMethods([]string{http.MethodGet}, fmt.Sprintf("/_cache_info"), []netutil.HandleContinued{handler.handleTimeout, handler.handleAuth, handler.cacheInfo}, nil)

	return nil
}

func (handler *DocumentHandler) cacheInfo(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	reqArgs := netutil.GetUrlQuery(r)

	dbName := reqArgs[UrlParamDbName]
	spaceName := reqArgs[UrlParamSpaceName]
	space, err := handler.client.Master().Cache().SpaceByCache(context.Background(), dbName, spaceName)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, 404, err.Error(), err.Error())
	} else {
		resp.SendJson(ctx, w, space)
	}
	return ctx, true
}

func (handler *DocumentHandler) handleAuth(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	if config.Conf().Global.SkipAuth {
		return ctx, true
	}

	headerData := r.Header.Get(headerAuthKey)

	if headerData == "" {
		log.Warn("user visit %s has err not auth value ", r.URL)
		resp.SendErrorRootCause(ctx, w, http.StatusNotFound, resp.ErrTypeAuthException, resp.ErrReasonAuthCodeNotFound)
		return ctx, false
	}

	username, password, err := util.AuthDecrypt(headerData)
	if err != nil {
		err := fmt.Errorf(resp.ErrReasonAuthDecryptFailed, err.Error())
		resp.SendError(ctx, w, http.StatusBadRequest, err.Error())
		return ctx, false
	}

	user, _ := handler.client.Master().Cache().UserByCache(ctx, username)
	if user == nil {
		log.Warn("user visit %s not found , name:[%s]  ", r.URL, username)
		resp.SendError(ctx, w, http.StatusBadRequest, resp.ErrReasonUserNotFound)
		return ctx, false
	}
	if user.Password != password {
		log.Error("auth password not matched")
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, resp.ErrTypeAuthException, resp.ErrReasonAuthFailed)
		return ctx, false
	}

	return ctx, true
}

func (handler *DocumentHandler) handleTimeout(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	reqArgs := netutil.GetUrlQuery(r)

	timeoutStr := reqArgs[UrlQueryTimeout]

	if timeoutStr != "" {
		base := time.Second
		switch timeoutStr[len(timeoutStr)-1] {
		case 'm':
			timeoutStr = string(timeoutStr[:len(timeoutStr)-1])
			base = time.Minute
		case 's':
			timeoutStr = string(timeoutStr[:len(timeoutStr)-1])
		case 'h':
			timeoutStr = string(timeoutStr[:len(timeoutStr)-1])
			base = time.Hour
		}

		if timeout, err := cast.ToInt64E(timeoutStr); err != nil {
			log.Error("parse:[timeoutStr] timeout err , it must int value:[%s]", timeoutStr, reqArgs[UrlQueryTimeout])
		} else {
			ctx, _ = context.WithTimeout(ctx, time.Duration(timeout*int64(base)))
		}
	}

	return ctx, true
}

func (handler *DocumentHandler) handleClusterInfo(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {

	versionLayer := make(map[string]interface{})
	versionLayer["build_version"] = config.GetBuildVersion()
	versionLayer["build_time"] = config.GetBuildTime()
	versionLayer["commit_id"] = config.GetCommitID()

	layer := make(map[string]interface{})
	layer["name"] = config.Conf().Global.Name
	layer["cluster_name"] = config.Conf().Global.Name
	layer["cluster_uuid"] = ""
	layer["version"] = versionLayer
	layer["tagline"] = ""

	resp.SendJson(ctx, w, layer)
	return ctx, true
}

func (handler *DocumentHandler) handleGetSpaceMapping(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	dbName := params.ByName(UrlParamDbName)
	spaceName := params.ByName(UrlParamSpaceName)

	space, err := handler.docService.getSpace(ctx, dbName, spaceName)
	if err != nil {
		if pkg.ErrCode(err) == pkg.ERRCODE_SPACE_NOTEXISTS {
			resp.SendErrorRootCause(ctx, w, http.StatusNotFound, "", err.Error())
		} else {
			resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		}
		return ctx, true
	}

	jsonMap, err := cbjson.ByteToJsonMap(space.Properties)
	if err != nil {
		resp.SendError(ctx, w, http.StatusBadRequest, err.Error())
		return ctx, true
	}

	// properties layer
	propertiesLayer := make(map[string]interface{})
	propertiesLayer["properties"] = jsonMap
	// space layer
	spaceLayer := make(map[string]interface{})
	spaceLayer[space.Name] = propertiesLayer
	// mappings layer
	mappingsLayer := make(map[string]interface{})
	mappingsLayer["mappings"] = spaceLayer
	// db layer
	dbLayer := make(map[string]interface{})
	dbLayer[dbName] = mappingsLayer

	resp.SendJson(ctx, w, dbLayer)
	return ctx, true
}

func (handler *DocumentHandler) handleReplaceDoc(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	defer monitor.Profiler("handleReplaceDoc", startTime)
	method := r.Method
	dbName := params.ByName(UrlParamDbName)
	spaceName := params.ByName(UrlParamSpaceName)
	docID := params.ByName(UrlParamDocID)

	// check method and docID
	if method == "PUT" && docID == "" {
		resp.SendErrorMethodNotAllowed(ctx, w, r.URL.Path, method, http.MethodPost)
		return ctx, true
	}

	idIsLong := false
	space, err := handler.docService.getSpace(ctx, dbName, spaceName)
	if err != nil {
		resp.SendError(ctx, w, http.StatusBadRequest, err.Error())
		return ctx, true
	} else {
		idType := space.Engine.IdType
		if idType != "" && ("long" == idType || "Long" == idType) {
			idIsLong = true
		}
	}

	var docIdString string
	if docID == "" {
		//docID = uuid.FlakeUUID()
		/*n, err := snowflake.NewNode(1)
		if err != nil {
			fmt.Errorf("snowflake.NewNode error: (%s)", err.Error())
		} else {
			docId64 = n.Generate().Int64()
		}*/
		docIDUUID := uuid.FlakeUUID()
		docIdMd5 := GetMD5Encode(docIDUUID)
		bi := big.NewInt(0)
		before := docIdMd5[0:16]
		after := docIdMd5[16:32]

		bi.SetString(before, 16)
		beforeInt64 := bi.Int64()
		bi.SetString(after, 16)
		afterInt64 := bi.Int64()

		docId64 := beforeInt64 ^ afterInt64
		docIdString = strconv.FormatInt(docId64, 10)
	} else {
		if idIsLong {
			result := regularutil.StringCheckNum(docID)
			if !result {
				resp.SendError(ctx, w, http.StatusBadRequest, "space idType is long but input docId is not number")
				return ctx, true
			}
		}
		docIdString = docID
	}

	reqArgs := netutil.GetUrlQuery(r)
	reqBody, err := netutil.GetReqBody(r)
	if err != nil {
		resp.SendError(ctx, w, http.StatusBadRequest, err.Error())
		return ctx, true
	}

	if reqArgs[UrlQueryOpType] == "create" {
		docResult := handler.docService.createDoc(ctx, dbName, spaceName, docIdString, reqArgs, reqBody)
		writeResponse := response.WriteResponse{docResult}
		bs, err := writeResponse.ToContent(dbName, spaceName, idIsLong)
		if err != nil {
			resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
			return ctx, true
		}

		resp.SendJsonBytes(ctx, w, bs)
		return ctx, true
	}

	if string(reqBody) == "" {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, resp.ErrTypeParseException, resp.ErrReasonRequestBodyIsRequired)
		return ctx, true
	}

	docResult := handler.docService.replaceDoc(ctx, dbName, spaceName, docIdString, reqArgs, reqBody)
	writeResponse := response.WriteResponse{docResult}
	bs, err := writeResponse.ToContent(dbName, spaceName, idIsLong)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}
	resp.SendJsonBytes(ctx, w, bs)
	return ctx, true
}

func (handler *DocumentHandler) handleCreateDoc(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	defer monitor.Profiler("handleCreateDoc", startTime)

	dbName := params.ByName(UrlParamDbName)
	spaceName := params.ByName(UrlParamSpaceName)
	docID := params.ByName(UrlParamDocID)

	if docID == "" {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "docId is null")
		return ctx, true
	}

	reqArgs := netutil.GetUrlQuery(r)
	reqBody, err := netutil.GetReqBody(r)
	if err != nil {
		resp.SendError(ctx, w, http.StatusBadRequest, err.Error())
		return ctx, true
	}

	idIsLong := false
	var idType string
	space, err := handler.docService.getSpace(ctx, dbName, spaceName)
	if err != nil {
		resp.SendError(ctx, w, http.StatusBadRequest, err.Error())
		return ctx, true
	} else {
		idType = space.Engine.IdType
		if idType != "" && ("long" == idType || "Long" == idType) {
			idIsLong = true
		}
	}

	var docIdString string
	if docID == "" {
		//docID = uuid.FlakeUUID()
		/*n, err := snowflake.NewNode(1)
		if err != nil {
			fmt.Errorf("snowflake.NewNode error: (%s)", err.Error())
		} else {
			docId64 = n.Generate().Int64()
		}*/
		docIDUUID := uuid.FlakeUUID()
		docIdMd5 := GetMD5Encode(docIDUUID)
		bi := big.NewInt(0)
		before := docIdMd5[0:16]
		after := docIdMd5[16:32]

		bi.SetString(before, 16)
		beforeInt64 := bi.Int64()
		bi.SetString(after, 16)
		afterInt64 := bi.Int64()

		docId64 := beforeInt64 ^ afterInt64
		docIdString = strconv.FormatInt(docId64, 10)
	} else {
		if idIsLong {
			result := regularutil.StringCheckNum(docID)
			if !result {
				resp.SendError(ctx, w, http.StatusBadRequest, "space idType is long but input docId is not number")
				return ctx, true
			}
		}
		docIdString = docID
	}

	docResult := handler.docService.createDoc(ctx, dbName, spaceName, docIdString, reqArgs, reqBody)
	writeResponse := response.WriteResponse{docResult}
	bs, err := writeResponse.ToContent(dbName, spaceName, idIsLong)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}

	resp.SendJsonBytes(ctx, w, bs)
	return ctx, true
}

func (handler *DocumentHandler) handleUpdateDoc(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	defer monitor.Profiler("handleUpdateDoc", startTime)
	method := r.Method
	dbName := params.ByName(UrlParamDbName)
	spaceName := params.ByName(UrlParamSpaceName)
	docID := params.ByName(UrlParamDocID)

	// check method
	if method == "PUT" {
		resp.SendErrorMethodNotAllowed(ctx, w, r.URL.Path, method, http.MethodPost)
		return ctx, false
	}

	reqArgs := netutil.GetUrlQuery(r)
	reqBody, err := netutil.GetReqBody(r)
	if err != nil {
		resp.SendError(ctx, w, http.StatusBadRequest, err.Error())
		return ctx, false
	}

	if reqArgs[UrlQueryVersion] != "" && reqArgs[UrlQueryRetryOnConflict] != "" {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "can't provide both retry_on_conflict and a specific version")
		return ctx, false
	}

	jsonMap, err := cbjson.ByteToJsonMap(reqBody)
	if err != nil {
		resp.SendError(ctx, w, http.StatusBadRequest, err.Error())
		return ctx, false
	}

	doc, err := jsonMap.GetJsonValBytes("doc")
	if err != nil {
		resp.SendError(ctx, w, http.StatusBadRequest, err.Error())
		return ctx, false
	}

	if docID == "" {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "docId is null")
		return ctx, true
	}

	idIsLong := false
	var idType string
	space, err := handler.docService.getSpace(ctx, dbName, spaceName)
	if err != nil {
		resp.SendError(ctx, w, http.StatusBadRequest, err.Error())
		return ctx, true
	} else {
		idType = space.Engine.IdType
		if idType != "" && ("long" == idType || "Long" == idType) {
			idIsLong = true
		}
	}

	if idIsLong {
		result := regularutil.StringCheckNum(docID)
		if !result {
			resp.SendError(ctx, w, http.StatusBadRequest, "space idType is long but input docId is not number")
			return ctx, true
		}
	}

	docResult := handler.docService.mergeDoc(ctx, dbName, spaceName, docID, reqArgs, doc)

	writeResponse := response.WriteResponse{docResult}
	bs, err := writeResponse.ToContent(dbName, spaceName, idIsLong)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, false
	}

	resp.SendJsonBytes(ctx, w, bs)
	return ctx, true
}

func (handler *DocumentHandler) handleDeleteDoc(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	defer monitor.Profiler("handleDeleteDoc", startTime)

	dbName := params.ByName(UrlParamDbName)
	spaceName := params.ByName(UrlParamSpaceName)
	docID := params.ByName(UrlParamDocID)

	reqArgs := netutil.GetUrlQuery(r)

	if docID == "" {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "docId is null")
		return ctx, true
	}

	idIsLong := false
	var idType string
	space, err := handler.docService.getSpace(ctx, dbName, spaceName)
	if err != nil {
		resp.SendError(ctx, w, http.StatusBadRequest, err.Error())
		return ctx, true
	} else {
		idType = space.Engine.IdType
		if idType != "" && ("long" == idType || "Long" == idType) {
			idIsLong = true
		}
	}

	if idIsLong {
		result := regularutil.StringCheckNum(docID)
		if !result {
			resp.SendError(ctx, w, http.StatusBadRequest, "space idType is long but input docId is not number")
			return ctx, true
		}
	}

	docResult := handler.docService.deleteDoc(ctx, dbName, spaceName, docID, reqArgs)

	writeResponse := response.WriteResponse{docResult}
	bs, err := writeResponse.ToContent(dbName, spaceName, idIsLong)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}

	resp.SendJsonBytes(ctx, w, bs)
	return ctx, true
}

func (handler *DocumentHandler) handleGetDoc(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	defer monitor.Profiler("handleGetDoc", startTime)
	dbName := params.ByName(UrlParamDbName)
	spaceName := params.ByName(UrlParamSpaceName)
	docID := params.ByName(UrlParamDocID)

	reqArgs := netutil.GetUrlQuery(r)

	if docID == "" {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "docId is null")
		return ctx, true
	}

	idIsLong := false
	var idType string
	space, err := handler.docService.getSpace(ctx, dbName, spaceName)
	if err != nil {
		resp.SendError(ctx, w, http.StatusBadRequest, err.Error())
		return ctx, true
	} else {
		idType = space.Engine.IdType
		if idType != "" && ("long" == idType || "Long" == idType) {
			idIsLong = true
		}
	}

	if idIsLong {
		result := regularutil.StringCheckNum(docID)
		if !result {
			resp.SendError(ctx, w, http.StatusBadRequest, "space idType is long but input docId is not number")
			return ctx, true
		}
	}

	docResult := handler.docService.getDoc(ctx, dbName, spaceName, docID, reqArgs)
	bs, err := docResult.ToContent(dbName, spaceName, idIsLong)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}

	resp.SendJsonBytes(ctx, w, bs)
	return ctx, true
}

func (handler *DocumentHandler) handleBulk(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	paramTime := time.Now()
	defer monitor.Profiler("handleBulk", paramTime)

	dbName := params.ByName(UrlParamDbName)
	spaceName := params.ByName(UrlParamSpaceName)

	reqArgs := netutil.GetUrlQuery(r)
	reqBody, err := netutil.GetReqBody(r)
	if err != nil {
		resp.SendError(ctx, w, http.StatusBadRequest, err.Error())
		return ctx, false
	}

	idIsLong := false
	space, err := handler.docService.getSpace(ctx, dbName, spaceName)
	if err != nil {
		resp.SendError(ctx, w, http.StatusBadRequest, err.Error())
		return ctx, false
	} else {
		idType := space.Engine.IdType
		if idType != "" && ("long" == idType || "Long" == idType) {
			idIsLong = true
		}
	}

	t1 := time.Now()
	writeResponse, err := handler.docService.bulk(ctx, dbName, spaceName, reqArgs, reqBody, idIsLong)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}
	t2 := time.Now()

	bulkResponse := &response.BulkResponse{Items: writeResponse}
	bs, err := bulkResponse.ToContent(t2.Sub(t1).Nanoseconds(), idIsLong)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}
	resp.SendJsonBytes(ctx, w, bs)

	engTime := time.Now()

	gammaCost := int64(0)
	psStoreWriteCost := int64(0)
	psHandlerCost := int64(0)
	clientPsPCost := int64(0)
	docSCost := int64(0)
	for _, item := range bulkResponse.Items {
		if item.ItemValue.DocResult.CostTime != nil {
			gammaEndTime := item.ItemValue.DocResult.CostTime.GammaEndTime
			gammaStartTime := item.ItemValue.DocResult.CostTime.GammaStartTime
			gammaCost += int64(gammaEndTime.Sub(gammaStartTime))
			psSWStartTime := item.ItemValue.DocResult.CostTime.PsSWStartTime
			psSWEndTime := item.ItemValue.DocResult.CostTime.PsSWEndTime
			psStoreWriteCost += int64(psSWEndTime.Sub(psSWStartTime))
			psHandlerStartTime := item.ItemValue.DocResult.CostTime.PsHandlerStartTime
			psHandlerEndTime := item.ItemValue.DocResult.CostTime.PsHandlerEndTime
			psHandlerCost += int64(psHandlerEndTime.Sub(psHandlerStartTime))
			clientPsPStartTime := item.ItemValue.DocResult.CostTime.ClientPsPStartTime
			clientPsPEndTime := item.ItemValue.DocResult.CostTime.ClientPsPEndTime
			clientPsPCost += int64(clientPsPEndTime.Sub(clientPsPStartTime))
			docSStartTime := item.ItemValue.DocResult.CostTime.DocSStartTime
			docSEndTime := item.ItemValue.DocResult.CostTime.DocSEndTime
			docSCost += int64(docSEndTime.Sub(docSStartTime))
		}
	}

	log.Info("handleBulk gamma cost :[%d] pssw use cost:[%d] pshandler use cost :[%d] clientps cost :[%d] "+
		"docserice cost :[%d] bulk cost:[%d] param cost:[%d] total cost:[%d]",
		gammaCost/int64(time.Millisecond),
		psStoreWriteCost/int64(time.Millisecond),
		psHandlerCost/int64(time.Millisecond),
		clientPsPCost/int64(time.Millisecond),
		docSCost/int64(time.Millisecond),
		int64((t2.Sub(t1))/time.Millisecond),
		int64(t1.Sub(paramTime)/time.Millisecond),
		int64((engTime.Sub(paramTime))/time.Millisecond))
	return ctx, true
}

func (handler *DocumentHandler) handleMSearchIDsDoc(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	defer monitor.Profiler("handleMSearchIDsDoc", startTime)

	dbName := params.ByName(UrlParamDbName)
	spaceName := params.ByName(UrlParamSpaceName)

	reqArgs := netutil.GetUrlQuery(r)
	reqBody, err := netutil.GetReqBody(r)
	if err != nil {
		resp.SendError(ctx, w, http.StatusBadRequest, err.Error())
		return ctx, true
	}

	searchRequest := request.NewSearchRequest(ctx, uuid.FlakeUUID())
	if len(reqBody) != 0 {
		err := cbjson.Unmarshal(reqBody, searchRequest.SearchDocumentRequest)
		if err != nil {
			resp.SendError(ctx, w, http.StatusBadRequest, err.Error())
			return ctx, true
		}
	}

	var clientType client.ClientType

	switch reqArgs[ClientTypeValue] {
	case "leader", "":
		clientType = client.LEADER
	case "random":
		clientType = client.RANDOM
	case "not_leader":
		clientType = client.NOT_LEADER
	default:
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", fmt.Sprintf("client_type err param:[%s] , it use `leader` or `random`", reqArgs[ClientTypeValue]))
		return ctx, true
	}

	t1 := time.Now()
	searchResponses, nameCache, err := handler.docService.mSearchIDs(ctx, dbName, spaceName, searchRequest, clientType)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}
	t2 := time.Now()

	idIsLong := false
	space, err := handler.docService.getSpace(ctx, dbName, spaceName)
	if err != nil {
		resp.SendError(ctx, w, http.StatusBadRequest, err.Error())
		return ctx, true
	} else {
		idType := space.Engine.IdType
		if idType != "" && ("long" == idType || "Long" == idType) {
			idIsLong = true
		}
	}

	bs, err := searchResponses.ToContentIds(searchRequest.From, *searchRequest.Size, nameCache, t2.Sub(t1), idIsLong)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}

	/*bs, err := handler.docService.mSearchIDs(ctx, dbName, spaceName, searchRequest, clientType)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}*/

	resp.SendJsonBytes(ctx, w, bs)
	return ctx, true
}

func (handler *DocumentHandler) handleMSearchForIDsDoc(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	dbName := params.ByName(UrlParamDbName)
	spaceName := params.ByName(UrlParamSpaceName)

	reqArgs := netutil.GetUrlQuery(r)
	reqBody, err := netutil.GetReqBody(r)
	if err != nil {
		resp.SendError(ctx, w, http.StatusBadRequest, err.Error())
		return ctx, true
	}

	searchRequest := request.NewSearchRequest(ctx, uuid.FlakeUUID())
	if len(reqBody) != 0 {
		err := cbjson.Unmarshal(reqBody, searchRequest.SearchDocumentRequest)
		if err != nil {
			resp.SendError(ctx, w, http.StatusBadRequest, err.Error())
			return ctx, true
		}
	}

	var clientType client.ClientType

	switch reqArgs[ClientTypeValue] {
	case "leader", "":
		clientType = client.LEADER
	case "random":
		clientType = client.RANDOM
	case "not_leader":
		clientType = client.NOT_LEADER
	default:
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", fmt.Sprintf("client_type err param:[%s] , it use `leader` or `random`", reqArgs[ClientTypeValue]))
		return ctx, true
	}

	bs, err := handler.docService.mSearchForIDs(ctx, dbName, spaceName, searchRequest, clientType)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}

	resp.SendJsonBytes(ctx, w, bs)
	return ctx, true
}

func (handler *DocumentHandler) handleMSearchDoc(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	defer monitor.Profiler("handleMSearchDoc", startTime)

	dbName := params.ByName(UrlParamDbName)
	spaceName := params.ByName(UrlParamSpaceName)

	reqArgs := netutil.GetUrlQuery(r)
	reqBody, err := netutil.GetReqBody(r)
	if err != nil {
		resp.SendError(ctx, w, http.StatusBadRequest, err.Error())
		return ctx, true
	}

	searchRequest := request.NewSearchRequest(ctx, uuid.FlakeUUID())
	if len(reqBody) != 0 {
		err := cbjson.Unmarshal(reqBody, searchRequest.SearchDocumentRequest)
		if err != nil {
			resp.SendError(ctx, w, http.StatusBadRequest, err.Error())
			return ctx, true
		}
	}

	if reqArgs[UrlQueryFrom] != "" {
		searchRequest.From = cast.ToInt(reqArgs[UrlQueryFrom])
	}
	if reqArgs[UrlQuerySize] != "" {
		size := cast.ToInt(reqArgs[UrlQuerySize])
		searchRequest.Size = &size
	}

	var typedKeys bool
	if reqArgs[UrlQueryTypedKey] != "" {
		typedKeys = cast.ToBool(reqArgs[UrlQueryTypedKey])
	}

	if reqArgs[UrlQueryURISort] != "" {
		sortQ := strings.Split(reqArgs[UrlQueryURISort], ":")
		const sortObj = `[{"{{.Condition}}": {"order": "{{.Order}}"}}]`

		type SortObj struct {
			Condition string
			Order     string
		}

		obj := &SortObj{Condition: sortQ[0], Order: sortQ[1]}
		t := template.Must(template.New("sortObj").Parse(sortObj))
		var b bytes.Buffer
		err := t.Execute(&b, obj)
		if err != nil {
			resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
			return ctx, true
		}
		searchRequest.Sort = b.Bytes()
	}

	// set a default value
	if searchRequest.Size == nil {
		size := 10
		searchRequest.Size = &size
	}

	var clientType client.ClientType

	switch reqArgs[ClientTypeValue] {
	case "leader", "":
		clientType = client.LEADER
	case "random":
		clientType = client.RANDOM
	case "not_leader":
		clientType = client.NOT_LEADER
	default:
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", fmt.Sprintf("client_type err param:[%s] , it use `leader` or `random`", reqArgs[ClientTypeValue]))
		return ctx, true
	}

	t1 := time.Now()
	searchResponses, nameCache, err := handler.docService.mSearchDoc(ctx, dbName, spaceName, searchRequest, clientType)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}
	t2 := time.Now()

	idIsLong := false
	space, err := handler.docService.getSpace(ctx, dbName, spaceName)
	if err != nil {
		resp.SendError(ctx, w, http.StatusBadRequest, err.Error())
		return ctx, true
	} else {
		idType := space.Engine.IdType
		if idType != "" && ("long" == idType || "Long" == idType) {
			idIsLong = true
		}
	}

	bs, err := searchResponses.ToContent(searchRequest.From, *searchRequest.Size, nameCache, typedKeys, t2.Sub(t1), idIsLong)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}

	var maxTookID uint32
	var maxTook int64
	for _, sr := range searchResponses {
		if sr.MaxTook > maxTook {
			maxTook = sr.MaxTook
			maxTookID = sr.MaxTookID
		}
	}
	log.Info("msearch use time :[%d] . max partition:[%d] use time:[%d]", (t2.Sub(t1) / time.Millisecond), maxTookID, maxTook)

	resp.SendJsonBytes(ctx, w, bs)
	return ctx, true
}

func (handler *DocumentHandler) handleMSearchNewDoc(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	dbName := params.ByName(UrlParamDbName)
	spaceName := params.ByName(UrlParamSpaceName)

	reqArgs := netutil.GetUrlQuery(r)
	reqBody, err := netutil.GetReqBody(r)
	if err != nil {
		resp.SendError(ctx, w, http.StatusBadRequest, err.Error())
		return ctx, true
	}

	searchRequest := request.NewSearchRequest(ctx, uuid.FlakeUUID())
	if len(reqBody) != 0 {
		err := cbjson.Unmarshal(reqBody, searchRequest.SearchDocumentRequest)
		if err != nil {
			resp.SendError(ctx, w, http.StatusBadRequest, err.Error())
			return ctx, true
		}
	}

	if reqArgs[UrlQueryFrom] != "" {
		searchRequest.From = cast.ToInt(reqArgs[UrlQueryFrom])
	}
	if reqArgs[UrlQuerySize] != "" {
		size := cast.ToInt(reqArgs[UrlQuerySize])
		searchRequest.Size = &size
	}

	var typedKeys bool
	if reqArgs[UrlQueryTypedKey] != "" {
		typedKeys = cast.ToBool(reqArgs[UrlQueryTypedKey])
	}

	if reqArgs[UrlQueryURISort] != "" {
		sortQ := strings.Split(reqArgs[UrlQueryURISort], ":")
		const sortObj = `[{"{{.Condition}}": {"order": "{{.Order}}"}}]`

		type SortObj struct {
			Condition string
			Order     string
		}

		obj := &SortObj{Condition: sortQ[0], Order: sortQ[1]}
		t := template.Must(template.New("sortObj").Parse(sortObj))
		var b bytes.Buffer
		err := t.Execute(&b, obj)
		if err != nil {
			resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
			return ctx, true
		}
		searchRequest.Sort = b.Bytes()
	}

	// set a default value
	if searchRequest.Size == nil {
		size := 10
		searchRequest.Size = &size
	}

	var clientType client.ClientType

	switch reqArgs[ClientTypeValue] {
	case "leader", "":
		clientType = client.LEADER
	case "random":
		clientType = client.RANDOM
	case "not_leader":
		clientType = client.NOT_LEADER
	default:
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", fmt.Sprintf("client_type err param:[%s] , it use `leader` or `random`", reqArgs[ClientTypeValue]))
		return ctx, true
	}

	t1 := time.Now()
	searchResponses, nameCache, err := handler.docService.mSearchNewDoc(ctx, dbName, spaceName, searchRequest, clientType)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}
	t2 := time.Now()

	idIsLong := false
	space, err := handler.docService.getSpace(ctx, dbName, spaceName)
	if err != nil {
		resp.SendError(ctx, w, http.StatusBadRequest, err.Error())
		return ctx, true
	} else {
		idType := space.Engine.IdType
		if idType != "" && ("long" == idType || "Long" == idType) {
			idIsLong = true
		}
	}

	bs, err := searchResponses.ToContent(searchRequest.From, *searchRequest.Size, nameCache, typedKeys, t2.Sub(t1), idIsLong)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}

	var maxTookID uint32
	var maxTook int64
	for _, sr := range searchResponses {
		if sr.MaxTook > maxTook {
			maxTook = sr.MaxTook
			maxTookID = sr.MaxTookID
		}
	}
	log.Info("msearchnew use time :[%d] . max partition:[%d] use time:[%d]", (t2.Sub(t1) / time.Millisecond), maxTookID, maxTook)

	resp.SendJsonBytes(ctx, w, bs)
	return ctx, true
}

func (handler *DocumentHandler) handleDeleteByQuery(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	dbName := params.ByName(UrlParamDbName)
	spaceName := params.ByName(UrlParamSpaceName)

	reqArgs := netutil.GetUrlQuery(r)
	reqBody, err := netutil.GetReqBody(r)
	if err != nil {
		resp.SendError(ctx, w, http.StatusBadRequest, err.Error())
		return ctx, true
	}

	searchRequest := request.NewSearchRequest(ctx, uuid.FlakeUUID())
	if len(reqBody) != 0 {
		err := cbjson.Unmarshal(reqBody, searchRequest.SearchDocumentRequest)
		if err != nil {
			resp.SendError(ctx, w, http.StatusBadRequest, err.Error())
			return ctx, true
		}
	}

	if reqArgs[UrlQueryFrom] != "" {
		searchRequest.From = cast.ToInt(reqArgs[UrlQueryFrom])
	}
	if reqArgs[UrlQuerySize] != "" {
		size := cast.ToInt(reqArgs[UrlQuerySize])
		searchRequest.Size = &size
	}

	rep, _, err := handler.docService.deleteByQuery(ctx, dbName, spaceName, searchRequest)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}

	if rep.Err == nil {
		rep.Status = 200
	}

	resp.SendJson(ctx, w, rep)
	return ctx, true
}

func (handler *DocumentHandler) handleSearchDoc(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	defer monitor.Profiler("handleSearchDoc", startTime)

	dbName := params.ByName(UrlParamDbName)
	spaceName := params.ByName(UrlParamSpaceName)

	reqArgs := netutil.GetUrlQuery(r)
	reqBody, err := netutil.GetReqBody(r)
	if err != nil {
		resp.SendError(ctx, w, http.StatusBadRequest, err.Error())
		return ctx, true
	}

	searchRequest := request.NewSearchRequest(ctx, uuid.FlakeUUID())
	if len(reqBody) != 0 {
		err := cbjson.Unmarshal(reqBody, searchRequest.SearchDocumentRequest)
		if err != nil {
			resp.SendError(ctx, w, http.StatusBadRequest, err.Error())
			return ctx, true
		}
	} else {
		log.Error("len of reqBody: %d", len(reqBody))
		resp.SendError(ctx, w, http.StatusBadRequest, "param empty")
		return ctx, true
	}

	if reqArgs[UrlQueryFrom] != "" {
		searchRequest.From = cast.ToInt(reqArgs[UrlQueryFrom])
	}
	if reqArgs[UrlQuerySize] != "" {
		size := cast.ToInt(reqArgs[UrlQuerySize])
		searchRequest.Size = &size
	}

	var typedKeys bool
	if reqArgs[UrlQueryTypedKey] != "" {
		typedKeys = cast.ToBool(reqArgs[UrlQueryTypedKey])
	}

	if reqArgs[UrlQueryURISort] != "" {
		sortQ := strings.Split(reqArgs[UrlQueryURISort], ":")
		const sortObj = `[{"{{.Condition}}": {"order": "{{.Order}}"}}]`

		type SortObj struct {
			Condition string
			Order     string
		}

		obj := &SortObj{Condition: sortQ[0], Order: sortQ[1]}
		t := template.Must(template.New("sortObj").Parse(sortObj))
		var b bytes.Buffer
		err := t.Execute(&b, obj)
		if err != nil {
			resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
			return ctx, true
		}
		searchRequest.Sort = b.Bytes()
	}

	// set a default value
	if searchRequest.Size == nil {
		size := 10
		searchRequest.Size = &size
	}

	var clientType client.ClientType

	switch reqArgs[ClientTypeValue] {
	case "leader", "":
		clientType = client.LEADER
	case "random":
		clientType = client.RANDOM
	case "not_leader":
		clientType = client.NOT_LEADER
	default:
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", fmt.Sprintf("client_type err param:[%s] , it use `leader` or `random`", reqArgs[ClientTypeValue]))
		return ctx, true
	}

	t1 := time.Now()
	searchResponse, nameCache, err := handler.docService.searchDoc(ctx, dbName, spaceName, searchRequest, clientType)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}
	t2 := time.Now()

	idIsLong := false
	space, err := handler.docService.getSpace(ctx, dbName, spaceName)
	if err != nil {
		resp.SendError(ctx, w, http.StatusBadRequest, err.Error())
		return ctx, true
	} else {
		idType := space.Engine.IdType
		if idType != "" && ("long" == idType || "Long" == idType) {
			idIsLong = true
		}
	}

	bs, err := searchResponse.ToContent(searchRequest.From, *searchRequest.Size, nameCache, typedKeys, t2.Sub(t1), idIsLong)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}

	log.Info("search use time :[%d] . max partition:[%d] use time:[%d]", (t2.Sub(t1) / time.Millisecond), searchResponse.MaxTookID, searchResponse.MaxTook)

	resp.SendJsonBytes(ctx, w, bs)
	return ctx, true
}

func (handler *DocumentHandler) handleStreamSearchDoc(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	startTime := time.Now()
	defer monitor.Profiler("handleStreamSearchDoc", startTime)

	dbName := params.ByName(UrlParamDbName)
	spaceName := params.ByName(UrlParamSpaceName)

	reqArgs := netutil.GetUrlQuery(r)
	reqBody, err := netutil.GetReqBody(r)
	if err != nil {
		resp.SendError(ctx, w, http.StatusBadRequest, err.Error())
		return ctx, true
	}

	searchRequest := request.NewSearchRequest(ctx, uuid.FlakeUUID())
	if len(reqBody) != 0 {
		err := cbjson.Unmarshal(reqBody, searchRequest.SearchDocumentRequest)
		if err != nil {
			resp.SendError(ctx, w, http.StatusBadRequest, err.Error())
			return ctx, true
		}
	}

	if reqArgs[UrlQueryFrom] != "" {
		searchRequest.From = cast.ToInt(reqArgs[UrlQueryFrom])
	}
	if reqArgs[UrlQuerySize] != "" {
		size := cast.ToInt(reqArgs[UrlQuerySize])
		searchRequest.Size = &size
	}

	dsr, nameCache, err := handler.docService.streamSearchDoc(ctx, dbName, spaceName, searchRequest)
	defer dsr.Close()
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}

	idIsLong := false
	space, err := handler.docService.getSpace(ctx, dbName, spaceName)
	if err != nil {
		resp.SendError(ctx, w, http.StatusBadRequest, err.Error())
		return ctx, true
	} else {
		idType := space.Engine.IdType
		if idType != "" && ("long" == idType || "Long" == idType) {
			idIsLong = true
		}
	}

	var line = []byte("\n")

	defer func() {
		_, err = w.Write(line)
		if err != nil {
			log.Error(err.Error())
		}
	}()

	for {
		result, err := dsr.Next()
		if err != nil {
			resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
			return ctx, true
		}

		if result == nil {
			return ctx, true
		}

		names := nameCache[[2]int64{result.DB, result.Space}]
		content, err := result.ToContent(names[0], names[1], idIsLong)
		if err != nil {
			resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
			return ctx, true
		}
		_, err = w.Write(content)
		if err != nil {
			resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
			return ctx, true
		}
		_, err = w.Write(line)
		if err != nil {
			resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
			return ctx, true
		}
	}

}

func (handler *DocumentHandler) handleFlush(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	dbName := params.ByName(UrlParamDbName)
	spaceName := params.ByName(UrlParamSpaceName)

	shards, err := handler.docService.flush(ctx, dbName, spaceName)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}
	shardsBytes, err := shards.ToContent()
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}

	resp.SendJsonBytes(ctx, w, shardsBytes)
	return ctx, true
}

func (handler *DocumentHandler) handleForceMerge(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {
	dbName := params.ByName(UrlParamDbName)
	spaceName := params.ByName(UrlParamSpaceName)

	shards, err := handler.docService.forceMerge(ctx, dbName, spaceName)
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}
	shardsBytes, err := shards.ToContent()
	if err != nil {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", err.Error())
		return ctx, true
	}

	resp.SendJsonBytes(ctx, w, shardsBytes)
	return ctx, true
}

func (handler *DocumentHandler) namePasswordEncrypt(ctx context.Context, w http.ResponseWriter, r *http.Request, params netutil.UriParams) (context.Context, bool) {

	reqArgs := netutil.GetUrlQuery(r)

	userName := reqArgs["name"]
	password := reqArgs["password"]

	if userName == "" || password == "" {
		resp.SendErrorRootCause(ctx, w, http.StatusBadRequest, "", "url param must have ip:port?name=yourName&password=yourPassword")
		return ctx, true
	}

	resp.SendJsonHttpReplySuccess(ctx, w, util.AuthEncrypt(userName, password))
	return ctx, true
}

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
	"bufio"
	"context"
	"fmt"
	"strings"

	"github.com/smallnest/rpcx/share"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/client"
	pkg "github.com/vearch/vearch/proto"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/proto/pspb"
	"github.com/vearch/vearch/proto/request"
	"github.com/vearch/vearch/proto/response"
	"github.com/vearch/vearch/util/cbjson"
	"github.com/vearch/vearch/util/log"
	"github.com/vearch/vearch/util/uuid"
)

type docService struct {
	client *client.Client
}

func newDocService(client *client.Client) *docService {
	return &docService{
		client: client,
	}
}

func (this *docService) createDoc(ctx context.Context, dbName string, spaceName string, docID string, reqArgs RawReqArgs, reqBody RawReqBody) *response.DocResult {
	if _, ok := reqArgs[UrlQueryRefresh]; !ok {
		reqArgs[UrlQueryRefresh] = "false"
	}

	ctx = context.WithValue(ctx, share.ReqMetaDataKey, map[string]string{UrlQueryRefresh: reqArgs[UrlQueryRefresh]})
	return this.client.PS().Be(ctx).Space(dbName, spaceName).SetRoutingValue(reqArgs[UrlQueryRouting]).CreateDoc(docID, reqBody)
}

func (this *docService) mergeDoc(ctx context.Context, dbName string, spaceName string, docID string, reqArgs RawReqArgs, reqBody []byte) *response.DocResult {
	version := int64(0)
	if reqArgs[UrlQueryVersion] != "" {
		version = cast.ToInt64(reqArgs[UrlQueryVersion])
	}

	tryTimes := 0
	if reqArgs[UrlQueryRetryOnConflict] != "" {
		tryTimes = cast.ToInt(reqArgs[UrlQueryRetryOnConflict])
	}
	if _, ok := reqArgs[UrlQueryRefresh]; !ok {
		reqArgs[UrlQueryRefresh] = "false"
	}
	ctx = context.WithValue(ctx, share.ReqMetaDataKey, map[string]string{UrlQueryRefresh: reqArgs[UrlQueryRefresh]})
	return this.client.PS().Be(ctx).Space(dbName, spaceName).SetRoutingValue(reqArgs[UrlQueryRouting]).SetWriteTryTimes(tryTimes).MergeDoc(docID, reqBody, version) //TODO make sure version how to use
}

func (this *docService) replaceDoc(ctx context.Context, dbName string, spaceName string, docID string, reqArgs RawReqArgs, reqBody []byte) *response.DocResult {
	if _, ok := reqArgs[UrlQueryRefresh]; !ok {
		reqArgs[UrlQueryRefresh] = "false"
	}
	ctx = context.WithValue(ctx, share.ReqMetaDataKey, map[string]string{UrlQueryRefresh: reqArgs[UrlQueryRefresh]})
	return this.client.PS().Be(ctx).Space(dbName, spaceName).SetRoutingValue(reqArgs[UrlQueryRouting]).ReplaceDoc(docID, reqBody)
}

func (this *docService) deleteDoc(ctx context.Context, dbName string, spaceName string, docID string, reqArgs RawReqArgs) *response.DocResult {
	if _, ok := reqArgs[UrlQueryRefresh]; !ok {
		reqArgs[UrlQueryRefresh] = "false"
	}
	ctx = context.WithValue(ctx, share.ReqMetaDataKey, map[string]string{UrlQueryRefresh: reqArgs[UrlQueryRefresh]})
	return this.client.PS().Be(ctx).Space(dbName, spaceName).SetRoutingValue(reqArgs[UrlQueryRouting]).DeleteDoc(docID)
}

func (this *docService) getDoc(ctx context.Context, dbName string, spaceName string, docID string, reqArgs RawReqArgs) *response.DocResult {
	return this.client.PS().B().Space(dbName, spaceName).SetRoutingValue(reqArgs[UrlQueryRouting]).GetDoc(docID)
}

func (this *docService) getDocs(ctx context.Context, dbName string, spaceName string, docIDs []string, reqArgs RawReqArgs) response.DocResults {
	return this.client.PS().B().Space(dbName, spaceName).SetRoutingValue(reqArgs[UrlQueryRouting]).GetDocs(docIDs)
}

func (this *docService) mSearchIDs(ctx context.Context, dbName string, spaceName string, searchRequest *request.SearchRequest, clientType client.ClientType) ([]byte, error) {
	searchSpaces, _, err := this.parseDBSpacePair(ctx, dbName, spaceName)
	if err != nil {
		return  nil, err
	}

	if len(searchSpaces) == 0 {
		return nil, pkg.CodeErr(pkg.ERRCODE_SPACE_NOTEXISTS)
	}

	return this.client.PS().Be(ctx).MultipleSpaceByType(searchSpaces, clientType).MSearchIDs(searchRequest)
}

func (this *docService) mSearchDoc(ctx context.Context, dbName string, spaceName string, searchRequest *request.SearchRequest, clientType client.ClientType) (response.SearchResponses, response.NameCache, error) {
	searchSpaces, nameCache, err := this.parseDBSpacePair(ctx, dbName, spaceName)
	if err != nil {
		return nil, nil, err
	}

	if len(searchSpaces) == 0 {
		return nil, nil, pkg.CodeErr(pkg.ERRCODE_SPACE_NOTEXISTS)
	}

	return this.client.PS().Be(ctx).MultipleSpaceByType(searchSpaces, clientType).MSearch(searchRequest), nameCache, nil
}

func (this *docService) deleteByQuery(ctx context.Context, dbName string, spaceName string, searchRequest *request.SearchRequest) (*response.Response, response.NameCache, error) {
	searchSpaces, nameCache, err := this.parseDBSpacePair(ctx, dbName, spaceName)
	if err != nil {
		return nil, nil, err
	}

	if len(searchSpaces) == 0 {
		return nil, nil, pkg.CodeErr(pkg.ERRCODE_SPACE_NOTEXISTS)
	}

	return this.client.PS().Be(ctx).MultipleSpace(searchSpaces).DeleteByQuery(searchRequest), nameCache, nil
}

func (this *docService) searchDoc(ctx context.Context, dbName string, spaceName string, searchRequest *request.SearchRequest, clientType client.ClientType) (*response.SearchResponse, response.NameCache, error) {
	searchSpaces, nameCache, err := this.parseDBSpacePair(ctx, dbName, spaceName)
	if err != nil {
		return nil, nil, err
	}

	if len(searchSpaces) == 0 {
		return nil, nil, pkg.CodeErr(pkg.ERRCODE_SPACE_NOTEXISTS)
	}

	return this.client.PS().Be(ctx).MultipleSpaceByType(searchSpaces, clientType).Search(searchRequest), nameCache, nil
}

//it make uri to db space pair like db1,db2/s1,s1  it will return db1/s1 , db2/s1
func (this *docService) parseDBSpacePair(ctx context.Context, dbName string, spaceName string) ([][2]string, response.NameCache, error) {
	var searchSpaces [][2]string
	dbNames := strings.Split(dbName, ",")
	spaceNames := strings.Split(spaceName, ",")
	nameCache := make(response.NameCache)
	if len(dbNames) != len(spaceNames) {
		return nil, nil, fmt.Errorf("in uri dbNames:[%v] must length equals spaceNames:[%v] , example _search/db1,db1/table1,table2", dbNames, spaceNames)
	}
	for i, dbName := range dbNames {
		spaceName := spaceNames[i]
		if space, err := this.client.Master().Cache().SpaceByCache(ctx, dbName, spaceName); err == nil {
			key := [2]int64{int64(space.DBId), int64(space.Id)}
			if nameCache[key] == nil {
				nameCache[key] = []string{dbName, spaceName}
				searchSpaces = append(searchSpaces, [2]string{dbName, spaceName})
			}
		} else {
			log.Error("can not find db:[%s] space:[%s] for search err:[%s] ", dbName, spaceName, err.Error())
		}
	}
	return searchSpaces, nameCache, nil
}

func (this *docService) streamSearchDoc(ctx context.Context, dbName string, spaceName string, searchRequest *request.SearchRequest) (dsr *response.DocStreamResult, nameCache response.NameCache, err error) {

	var searchSpaces [][2]string

	dbNames := strings.Split(dbName, ",")
	spaceNames := strings.Split(spaceName, ",")

	nameCache = make(response.NameCache)

	for _, dbName = range dbNames {
		for _, spaceName = range spaceNames {
			if space, err := this.client.Master().Cache().SpaceByCache(ctx, dbName, spaceName); err == nil {
				key := [2]int64{int64(space.DBId), int64(space.Id)}
				if nameCache[key] == nil {
					nameCache[key] = []string{dbName, spaceName}
					searchSpaces = append(searchSpaces, [2]string{dbName, spaceName})
				}
			} else {
				log.Error("can not find db:[%s] space:[%s] for search err:[%s] ", dbName, spaceName, err.Error())
			}
		}
	}

	if len(searchSpaces) == 0 {
		return nil, nil, pkg.CodeErr(pkg.ERRCODE_SPACE_NOTEXISTS)
	}

	return this.client.PS().Be(ctx).MultipleSpace(searchSpaces).StreamSearch(searchRequest), nameCache, nil
}

func (this *docService) flush(ctx context.Context, dbName string, spaceName string) (*response.Shards, error) {
	return this.client.PS().B().Space(dbName, spaceName).Flush()
}

func (this *docService) forceMerge(ctx context.Context, dbName string, spaceName string) (*response.Shards, error) {
	return this.client.PS().B().Space(dbName, spaceName).ForceMerge()
}

func (this *docService) bulk(ctx context.Context, dbName string, spaceName string, reqArgs RawReqArgs, reqBody []byte) ([]*response.BulkItemResponse, error) {
	var birs []*response.BulkItemResponse

	sr := strings.NewReader(string(reqBody))
	br := bufio.NewScanner(sr)
	for br.Scan() {
		line := string(br.Bytes())
		if len(line) < 1 {
			continue
		}
		jsonMap, err := cbjson.ByteToJsonMap(br.Bytes())
		if err != nil {
			return nil, err
		}

		indexJsonMap := jsonMap.GetJsonMap("index")
		createJsonMap := jsonMap.GetJsonMap("create")
		updateJsonMap := jsonMap.GetJsonMap("update")
		deleteJsonMap := jsonMap.GetJsonMap("delete")

		var realDbName, realSpaceName, routing, docID string
		var source []byte
		var opType pspb.OpType
		var version int64
		if indexJsonMap != nil {
			realDbName = indexJsonMap.GetJsonValString("_index")
			if realDbName == "" {
				realDbName = dbName
			}
			realSpaceName = indexJsonMap.GetJsonValString("_type")
			if realSpaceName == "" {
				realSpaceName = spaceName
			}
			docID = indexJsonMap.GetJsonValString("_id")
			routing = indexJsonMap.GetJsonValString("_routing")
			opType = pspb.OpType_REPLACE
			version = -1

			br.Scan()
			source = br.Bytes()
		} else if createJsonMap != nil {
			realDbName = createJsonMap.GetJsonValString("_index")
			if realDbName == "" {
				realDbName = dbName
			}
			realSpaceName = createJsonMap.GetJsonValString("_type")
			if realSpaceName == "" {
				realSpaceName = spaceName
			}
			docID = createJsonMap.GetJsonValString("_id")
			routing = createJsonMap.GetJsonValString("_routing")
			opType = pspb.OpType_CREATE
			version = 0

			br.Scan()
			source = br.Bytes()
		} else if updateJsonMap != nil {
			realDbName = updateJsonMap.GetJsonValString("_index")
			if realDbName == "" {
				realDbName = dbName
			}
			realSpaceName = updateJsonMap.GetJsonValString("_type")
			if realSpaceName == "" {
				realSpaceName = spaceName
			}

			docID = updateJsonMap.GetJsonValString("_id")
			routing = updateJsonMap.GetJsonValString("_routing")
			opType = pspb.OpType_MERGE
			version = 0

			br.Scan()
			docJsonMap, err := cbjson.ByteToJsonMap(br.Bytes())
			if err != nil {
				return nil, err
			}
			source, err = docJsonMap.GetJsonValBytes("doc")
			if err != nil {
				return nil, err
			}
		} else if deleteJsonMap != nil {
			realDbName = deleteJsonMap.GetJsonValString("_index")
			if realDbName == "" {
				realDbName = dbName
			}
			realSpaceName = deleteJsonMap.GetJsonValString("_type")
			if realSpaceName == "" {
				realSpaceName = spaceName
			}
			docID = deleteJsonMap.GetJsonValString("_id")
			routing = deleteJsonMap.GetJsonValString("_routing")
			opType = pspb.OpType_DELETE
			version = 0
		} else {
			continue
		}

		if realDbName == "" {
			return nil, fmt.Errorf("db name not found (%s)", line)
		}
		if realSpaceName == "" {
			return nil, fmt.Errorf("space name not found (%s)", line)
		}

		if docID == "" {
			docID = uuid.FlakeUUID()
		}

		slot := this.client.PS().B().Space(realDbName, realSpaceName).SetRoutingValue(routing).Slot(docID)
		docCmd := pspb.NewDocCmd(opType, docID, slot, source, version)
		defer func(docCmd *pspb.DocCmd) {
			go pspb.PutDocCmd(docCmd)
		}(docCmd)

		var docResult response.DocResultWrite
		resp := this.client.PS().B().Space(realDbName, realSpaceName).Bulk(docCmd)
		docResult = response.DocResultWrite{
			DbName:    realDbName,
			SpaceName: realSpaceName,
			DocResult: resp,
		}
		bir := &response.BulkItemResponse{OpType: opType, ItemValue: &docResult}
		birs = append(birs, bir)
	}

	return birs, nil
}

func (this *docService) getSpace(ctx context.Context, dbName string, spaceName string) (*entity.Space, error) {
	var err error

	dbID, err := this.client.Master().QueryDBName2Id(ctx, dbName)
	if err != nil {
		return nil, err
	}
	space, err := this.client.Master().QuerySpaceByName(ctx, dbID, spaceName)
	if err != nil {
		return nil, err
	}

	return space, nil
}

func (this *docService) createDb(ctx context.Context, dbName string) error {
	err := this.client.Master().CreateDb(ctx, dbName)
	if err != nil {
		return err
	}

	return nil
}

func (this *docService) createSpace(ctx context.Context, dbName string, spaceName string, mapping []byte) error {
	dbID, err := this.client.Master().QueryDBName2Id(ctx, dbName)
	if err != nil {
		return err
	}

	entitySpace := &entity.Space{}
	err = cbjson.Unmarshal(mapping, &entitySpace)
	if err != nil {
		return err
	}
	entitySpace.DBId = dbID
	entitySpace.Name = spaceName

	err = this.client.Master().CreateSpace(ctx, dbName, entitySpace)
	if err != nil {
		return err
	}

	return nil
}

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

package client

import (
	"bytes"
	"fmt"
	"github.com/vearch/vearch/proto/request"
	"github.com/vearch/vearch/proto/response"
	"github.com/vearch/vearch/ps/engine/sortorder"
	"sync"
	"time"

	"github.com/vearch/vearch/proto"
	"github.com/vearch/vearch/proto/pspb"
	"github.com/vearch/vearch/util/cbbytes"

	"github.com/spaolacci/murmur3"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/util/log"
	"runtime/debug"
)

type spaceSender struct {
	*sender
	routingValue  string
	writeTryTimes int
	clientType    ClientType
	db            string
	space         string
}

func (this *spaceSender) Type(clientType ClientType) *spaceSender {
	this.clientType = clientType
	return this
}

func (this *spaceSender) partitionSlot(slot entity.SlotID) *partitionSender {
	return &partitionSender{spaceSender: this, slot: slot}
}

//if user partitionId meas id not change , so retry is not a good idea ,
// example delete(ids...) this partitionID is changed , the slot will not same as groupIdMap
func (this *spaceSender) partitionId(pid entity.PartitionID) *partitionSender {
	return &partitionSender{spaceSender: this, pid: pid}
}

func (this *spaceSender) SetRoutingValue(routingValue string) *spaceSender {
	this.routingValue = routingValue
	return this
}

func (this *spaceSender) SetWriteTryTimes(times int) *spaceSender {
	this.writeTryTimes = times
	return this
}

func (this *spaceSender) GetDoc(id string) *response.DocResult {
	resp, err := this.partitionSlot(this.Slot(id)).getDoc(id)
	if err != nil {
		log.Error("Fail to search ps.  db[%s], space[%s]. err[%v]", this.db, this.space, err)
		if pkg.ErrCode(err) == pkg.ERRCODE_PARTITION_NOT_EXIST {
			this.ps.client.master.cliCache.DeleteSpaceCache(this.Ctx.GetContext(), this.db, this.space)
			resp, err = this.partitionSlot(this.Slot(id)).getDoc(id)
			if err != nil {
				return response.NewErrDocResult(id, err)
			}
			return resp
		}
		return response.NewErrDocResult(id, err)
	}
	return resp
}

//use this function by default slot hash [id , murmur3] ,
// if you want define your slot method ,please use SetRoutingValue to define it
// return entity.DocumentResponse
func (this *spaceSender) GetDocs(ids []string) response.DocResults {
	idMap, err := this.groupPartition(ids)
	if err != nil {
		return response.NewErrDocResults(ids, err)
	}

	var wg sync.WaitGroup
	respChain := make(chan response.DocResults, len(idMap))

	for pID, idArr := range idMap {
		wg.Add(1)
		go func(paritionID entity.PartitionID, idArr []string) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					respChain <- response.NewErrDocResults(idArr, err)
					log.Error(fmt.Sprint(r))
				}
			}()
			resp, err := this.partitionId(paritionID).getDocs(idArr)

			if err != nil {
				if pkg.ErrCode(err) == pkg.ERRCODE_PARTITION_NOT_EXIST {
					this.ps.client.master.cliCache.DeleteSpaceCache(this.Ctx.GetContext(), this.db, this.space)
					resp, err = this.partitionId(paritionID).getDocs(idArr)
					if err != nil {
						resp = response.NewErrDocResults(idArr, err)
					}

				} else {
					log.Error("Fail to search ps.  db[%s], space[%s]. err[%v]", this.db, this.space, err)
					resp = response.NewErrDocResults(idArr, err)
				}

			}

			respChain <- resp
		}(pID, idArr)

	}

	wg.Wait()
	close(respChain)

	result := make(response.DocResults, len(ids))
	resultIdMap := make(map[string]int, len(ids))
	for i, id := range ids {
		resultIdMap[id] = i
	}

	for rs := range respChain {
		for _, r := range rs {
			result[resultIdMap[r.Id]] = r
		}
	}

	return result
}

//search from space, by partitions
//clientType LEADER or RANDOM
// return entity.SearchResult
func (this *spaceSender) MSearchIDs(req *request.SearchRequest) ([]byte, error) {
	space, err := this.ps.Client().Master().cliCache.SpaceByCache(this.Ctx.GetContext(), this.db, this.space)
	if err != nil {
		return nil, err
	}
	return this.MSearchIDsByPartitions(space.Partitions, req)
}

//search from space, by partitions
//clientType LEADER or RANDOM
// return entity.SearchResult
func (this *spaceSender) MSearch(req *request.SearchRequest) response.SearchResponses {
	space, err := this.ps.Client().Master().cliCache.SpaceByCache(this.Ctx.GetContext(), this.db, this.space)
	if err != nil {
		return response.SearchResponses{response.NewSearchResponseErr(err)}
	}

	resp, err := this.MSearchByPartitions(space.Partitions, req)
	if err != nil {
		return response.SearchResponses{response.NewSearchResponseErr(err)}
	}
	return resp
}

func (this *spaceSender) MSearchIDsByPartitions(partitions []*entity.Partition, req *request.SearchRequest) (resp []byte, err error) {
	var wg sync.WaitGroup
	respChain := make(chan struct{
		reponse []byte
		err error
	}, len(partitions))

	for _, p := range partitions {
		wg.Add(1)
		go func(par *entity.Partition) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					log.Error("search has panic :[%s]", r)
					respChain <- struct{
						reponse []byte
						err error
					}{
						reponse : nil ,
						err :fmt.Errorf(cast.ToString(r)),
					}
				}
			}()
			resp, err := this.partitionId(par.Id).mSearchIDs(req)
			if err != nil {
				log.Error("Fail to search ps. partition[%d], db[%s], space[%s]. err[%v]", par.Id, this.db, this.space, err)
				if pkg.ErrCode(err) == pkg.ERRCODE_PARTITION_NOT_EXIST {
					this.ps.client.master.cliCache.DeleteSpaceCache(this.Ctx.GetContext(), this.db, this.space)
					resp, err = this.partitionId(par.Id).mSearchIDs(req)
					if err != nil {
						respChain <- struct{
							reponse []byte
							err error
						}{
							reponse : nil ,
							err :err,
						}
						return ;
					}
				}

			}
			respChain <- struct{
				reponse []byte
				err error
			}{
				reponse : resp ,
				err :err,
			}
		}(p)
	}

	wg.Wait()
	close(respChain)

	buf := bytes.Buffer{}

	for r := range respChain {

		if r.err != nil {
			return nil , r.err
		}

		if buf.Len() !=0{
			buf.WriteString("\n")
		}
		buf.Write(r.reponse)
	}
	return buf.Bytes(), nil
}

func (this *spaceSender) MSearchByPartitions(partitions []*entity.Partition, req *request.SearchRequest) (resp response.SearchResponses, err error) {

	var wg sync.WaitGroup
	respChain := make(chan response.SearchResponses, len(partitions))

	for _, p := range partitions {
		wg.Add(1)
		go func(par *entity.Partition) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					log.Error("search has panic :[%s]", r)
					respChain <- response.SearchResponses{newSearchResponseWithError(this.db, this.space, par.Id, fmt.Errorf(cast.ToString(r)))}
				}
			}()
			resp, err := this.partitionId(par.Id).mSearch(req)
			if err != nil {
				log.Error("Fail to search ps. partition[%d], db[%s], space[%s]. err[%v]", par.Id, this.db, this.space, err)
				resp = response.SearchResponses{newSearchResponseWithError(this.db, this.space, p.Id, err)}
				if pkg.ErrCode(err) == pkg.ERRCODE_PARTITION_NOT_EXIST {
					this.ps.client.master.cliCache.DeleteSpaceCache(this.Ctx.GetContext(), this.db, this.space)
					resp, err = this.partitionId(par.Id).mSearch(req)
					if err != nil {
						resp = response.SearchResponses{newSearchResponseWithError(this.db, this.space, p.Id, err)}
					}
				}

			}
			respChain <- resp
		}(p)
	}

	wg.Wait()
	close(respChain)

	var result response.SearchResponses
	for r := range respChain {
		if result == nil {
			result = r
			continue
		}
		var err error

		if err = this.mergeResultArr(result, r, req); err != nil {
			return nil, err
		}
	}

	return result, nil
}

func (this *spaceSender) DeleteByQuery(req *request.SearchRequest) *response.Response {
	space, err := this.ps.Client().Master().cliCache.SpaceByCache(this.Ctx.GetContext(), this.db, this.space)
	if err != nil {
		return &response.Response{Status: pkg.ErrCode(err), Err: err}
	}

	return this.DeleteByPartitions(space.Partitions, req)
}

//search from space, by partitions
//clientType LEADER or RANDOM
// return entity.SearchResult
func (this *spaceSender) Search(req *request.SearchRequest) *response.SearchResponse {
	space, err := this.ps.Client().Master().cliCache.SpaceByCache(this.Ctx.GetContext(), this.db, this.space)
	if err != nil {
		return response.NewSearchResponseErr(err)
	}

	resp, err := this.SearchByPartitions(space.Partitions, req)
	if err != nil {
		return response.NewSearchResponseErr(err)
	}
	return resp
}

func (this *spaceSender) DeleteByPartitions(partitions []*entity.Partition, req *request.SearchRequest) (resp *response.Response) {

	var wg sync.WaitGroup
	respChain := make(chan *response.Response, len(partitions))

	for _, p := range partitions {
		wg.Add(1)
		go func(par *entity.Partition) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					log.Error("search has panic :[%s]", r)
					respChain <- &response.Response{Status: pkg.ERRCODE_INTERNAL_ERROR, Err: fmt.Errorf(cast.ToString(r))}
				}
			}()
			resp := this.partitionId(par.Id).DeleteByQuery(req)
			if resp.Err != nil {
				log.Error("Fail to search ps. partition[%d], db[%s], space[%s]. err[%v]", par.Id, this.db, this.space, resp.Err)
				if pkg.ErrCode(resp.Err) == pkg.ERRCODE_PARTITION_NOT_EXIST {
					this.ps.client.master.cliCache.DeleteSpaceCache(this.Ctx.GetContext(), this.db, this.space)
					resp = this.partitionId(par.Id).DeleteByQuery(req)
				}

			}
			respChain <- resp
		}(p)
	}

	wg.Wait()
	close(respChain)

	var first *response.Response

	for r := range respChain {
		if first == nil {
			first = r
			continue
		}
		first.Resp = cast.ToInt(first.Resp) + cast.ToInt(r.Resp)
	}

	return first
}

func (this *spaceSender) SearchByPartitions(partitions []*entity.Partition, req *request.SearchRequest) (resp *response.SearchResponse, err error) {

	var wg sync.WaitGroup
	respChain := make(chan *response.SearchResponse, len(partitions))

	for _, p := range partitions {
		wg.Add(1)
		go func(par *entity.Partition) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					log.Error("search has panic :[%s]", r)
					respChain <- newSearchResponseWithError(this.db, this.space, par.Id, fmt.Errorf(cast.ToString(r)))
				}
			}()
			resp, err := this.partitionId(par.Id).search(req)
			if err != nil {
				log.Error("Fail to search ps. partition[%d], db[%s], space[%s]. err[%v]", par.Id, this.db, this.space, err)
				resp = newSearchResponseWithError(this.db, this.space, p.Id, err)
				if pkg.ErrCode(err) == pkg.ERRCODE_PARTITION_NOT_EXIST {
					this.ps.client.master.cliCache.DeleteSpaceCache(this.Ctx.GetContext(), this.db, this.space)
					resp, err = this.partitionId(par.Id).search(req)
					if err != nil {
						resp = newSearchResponseWithError(this.db, this.space, p.Id, err)
					}
				}

			}
			respChain <- resp
		}(p)
	}

	wg.Wait()
	close(respChain)

	space, err := this.ps.Client().Master().Cache().SpaceByCache(this.Ctx.GetContext(), this.db, this.space)
	if err != nil {
		return nil, err
	}

	sortOrder, err := req.SortOrder()
	if err != nil {
		return nil, err
	}

	if space.Engine.MetricType == "L2" {
		sortOrder = sortorder.SortOrder{&sortorder.SortScore{Desc: false}}
	}

	var first *response.SearchResponse
	for r := range respChain {
		if first == nil {
			first = r
			continue
		}
		err := first.Merge(r, sortOrder, req.From, *req.Size)
		if err != nil {
			return nil, err
		}
	}

	return first, nil
}

func (this *spaceSender) StreamSearch(req *request.SearchRequest) *response.DocStreamResult {
	dsr := response.NewDocStreamResult(this.Ctx.GetContext())

	space, err := this.ps.Client().Master().cliCache.SpaceByCache(this.Ctx.GetContext(), this.db, this.space)
	if err != nil {
		dsr.AddErr(err)
		return dsr
	}

	go func() {
		defer func() {
			dsr.AddDoc(nil)
		}()

		for _, p := range space.Partitions {
			this.partitionId(p.Id).streamSearch(req, dsr)
		}
	}()

	return dsr
}

// Flush space, all partiitons
func (this *spaceSender) Flush() (*response.Shards, error) {
	space, err := this.ps.client.master.cliCache.SpaceByCache(this.Ctx.GetContext(), this.db, this.space)
	if err != nil {
		return nil, err
	}
	resp, err := this.FlushByPartitions(space.Partitions)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (this *spaceSender) FlushByPartitions(partitions []*entity.Partition) (resp *response.Shards, err error) {

	var wg sync.WaitGroup
	respChain := make(chan bool, len(partitions))

	for _, p := range partitions {
		wg.Add(1)
		go func(par *entity.Partition) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					log.Error(string(debug.Stack()))
					log.Error(cast.ToString(r))
					respChain <- false
				}
			}()
			err := this.partitionId(par.Id).flush(LEADER)
			flag := true
			if err != nil {
				flag = false
				log.Error("Fail to flush. partition[%d], db[%s], space[%s], partitionId:[%d]. err[%v]", par.Id, this.db, this.space, par.Id, err)
				if pkg.ErrCode(err) == pkg.ERRCODE_PARTITION_NOT_EXIST {
					this.ps.client.master.cliCache.DeleteSpaceCache(this.Ctx.GetContext(), this.db, this.space)
					flag = this.partitionId(par.Id).flush(LEADER) == nil
				}

			}
			respChain <- flag
		}(p)
	}

	wg.Wait()
	close(respChain)

	resp = new(response.Shards)
	resp.Total = len(partitions)

	for res := range respChain {
		if res {
			resp.Successful++
		} else {
			resp.Failed++
		}
	}

	return resp, nil
}

// ForceMerge space, all partiitons
func (this *spaceSender) ForceMerge() (*response.Shards, error) {
	space, err := this.ps.client.master.cliCache.SpaceByCache(this.Ctx.GetContext(), this.db, this.space)
	if err != nil {
		return nil, err
	}
	resp, err := this.ForceMergeByPartitions(space.Partitions)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (this *spaceSender) ForceMergeByPartitions(partitions []*entity.Partition) (resp *response.Shards, err error) {

	var wg sync.WaitGroup
	respChain := make(chan bool, len(partitions))

	for _, p := range partitions {
		wg.Add(1)
		go func(par *entity.Partition) {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					log.Error(string(debug.Stack()))
					log.Error(cast.ToString(r))
					respChain <- false
				}
			}()
			err := this.partitionId(par.Id).forceMerge(ALL)

			flag := true
			if err != nil {
				flag = false
				log.Error("Fail to forceMerge. partition[%d], db[%s], space[%s], partitionId:[%d]. err[%v]", par.Id, this.db, this.space, par.Id, err)
				if pkg.ErrCode(err) == pkg.ERRCODE_PARTITION_NOT_EXIST {
					this.ps.client.master.cliCache.DeleteSpaceCache(this.Ctx.GetContext(), this.db, this.space)
					flag = this.partitionId(par.Id).forceMerge(ALL) == nil
				}

			}
			respChain <- flag
		}(p)
	}

	wg.Wait()
	close(respChain)

	resp = new(response.Shards)
	resp.Total = len(partitions)

	for res := range respChain {
		if res {
			resp.Successful++
		} else {
			resp.Failed++
		}
	}

	return resp, nil
}

//batch handler , if you use it you must make sure it is docs type is right by slot
func (this *spaceSender) Batch(docs []*pspb.DocCmd) (response.WriteResponse, error) {
	docMap, err := this.groupPartitionByDocs(docs)
	if err != nil {
		return nil, err
	}

	var writeResponse response.WriteResponse
	for pid, gDocs := range docMap {
		wrs, err := this.partitionId(pid).batch(gDocs)
		if err != nil {
			for _, gdoc := range gDocs {
				docResult := response.NewErrDocResult(gdoc.DocId, err)
				writeResponse = append(writeResponse, response.WriteResponse{docResult}...)
			}
		} else {
			writeResponse = append(writeResponse, *wrs...)
		}
	}
	return writeResponse, err
}

func (this *spaceSender) Write(doc *pspb.DocCmd) (*response.DocResult, error) {
	sender := this.partitionSlot(doc.Slot)
	resp, err := sender.write(doc)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

//default version is 1
func (this *spaceSender) CreateDoc(id string, source []byte) *response.DocResult {
	return this.writeAndRetry(pspb.NewDocCreateWithSlot(id, this.Slot(id), source))
}

// if version is zero  update version++
func (this *spaceSender) MergeDoc(id string, source []byte, version int64) *response.DocResult {
	return this.writeAndRetry(pspb.NewDocMergeWithSlot(id, this.Slot(id), source, version))
}

// relace the doc version will set 1
func (this *spaceSender) ReplaceDoc(id string, source []byte) *response.DocResult {
	return this.writeAndRetry(pspb.NewDocReplaceWithSlot(id, this.Slot(id), source, -1))
}

// if version == -1 same as replace, over write and not check version ,new version is 1 , if version == 0 , update version++
func (this *spaceSender) UpdateDoc(id string, source []byte, version int64) *response.DocResult {
	return this.writeAndRetry(pspb.NewDocReplaceWithSlot(id, this.Slot(id), source, version))
}

//delete doc without version check
func (this *spaceSender) DeleteDoc(id string) *response.DocResult {
	return this.writeAndRetry(pspb.NewDocDeleteWithSlot(id, this.Slot(id), 0))
}

// if version == 0 , delete not check version
func (this *spaceSender) DeleteDocWithVersion(id string, version int64) *response.DocResult {
	return this.writeAndRetry(pspb.NewDocDeleteWithSlot(id, this.Slot(id), version))
}

func (this *spaceSender) Bulk(doc *pspb.DocCmd) *response.DocResult {
	return this.writeAndRetry(doc)
}

func (this *spaceSender) writeAndRetry(doc *pspb.DocCmd) *response.DocResult {
	sender := this.partitionSlot(doc.Slot)

	tryTimes := 0
	for {
		resp, err := sender.write(doc)
		if err == nil {
			return resp
		}

		tryTimes++
		// if for cycle, break force
		if tryTimes > 10 {
			return response.NewErrDocResult(doc.DocId, err)
		}

		log.Error("client ps write doc error: %s", err.Error())
		if pkg.ErrCode(err) == pkg.ERRCODE_PARTITION_NOT_EXIST {
			if tryTimes > 5 {
				return response.NewErrDocResult(doc.DocId, err)
			}
			this.ps.client.master.cliCache.DeleteSpaceCache(this.Ctx.GetContext(), this.db, this.space)
			time.Sleep(1 * time.Second)
		} else if pkg.ErrCode(err) == pkg.ERRCODE_PULL_OUT_VERSION_NOT_MATCH {
			if this.writeTryTimes == 0 {
				return response.NewErrDocResult(doc.DocId, err)
			}

			if tryTimes > this.writeTryTimes {
				return response.NewErrDocResult(doc.DocId, err)
			}
		} else {
			return response.NewErrDocResult(doc.DocId, err)
		}

	}
}

func (this *spaceSender) groupPartition(ids []string) (map[entity.PartitionID][]string, error) {
	space, err := this.ps.client.master.cliCache.SpaceByCache(this.Ctx.GetContext(), this.db, this.space)
	if err != nil {
		return nil, err
	}
	result := make(map[entity.PartitionID][]string)

	if this.routingValue != "" {
		slot := entity.SlotID(murmur3.Sum32WithSeed([]byte(this.routingValue), 0))
		result[space.PartitionId(slot)] = ids
		return result, nil
	}

	for _, id := range ids {
		partitionId := space.PartitionId(entity.SlotID(murmur3.Sum32WithSeed([]byte(id), 0)))
		result[partitionId] = append(result[partitionId], id)
	}
	return result, nil
}

func doc2Ids(docs []*pspb.DocCmd) []string {
	ids := make([]string, len(docs))
	for i := 0; i < len(ids); i++ {
		ids[i] = docs[i].DocId
	}
	return ids
}

func (this *spaceSender) groupPartitionByDocs(docs []*pspb.DocCmd) (map[entity.PartitionID][]*pspb.DocCmd, error) {
	space, err := this.ps.client.master.cliCache.SpaceByCache(this.Ctx.GetContext(), this.db, this.space)
	if err != nil {
		return nil, err
	}
	result := make(map[entity.PartitionID][]*pspb.DocCmd)

	for _, doc := range docs {
		partitionId := space.PartitionId(doc.Slot)
		result[partitionId] = append(result[partitionId], doc)
	}
	return result, nil
}

func (this *spaceSender) Slot(docID string) uint32 {
	if this.routingValue != "" {
		return Slot(this.routingValue)
	}
	return Slot(docID)
}

func Slot(routingValue string) uint32 {
	return murmur3.Sum32WithSeed(cbbytes.StringToByte(routingValue), 0)
}

func (this *spaceSender) mergeResultArr(dest response.SearchResponses, src response.SearchResponses, req *request.SearchRequest) error {

	sortOrder, err := req.SortOrder()
	if err != nil {
		return fmt.Errorf("sort err [%s]", string(req.Sort))
	}

	space, err := this.ps.Client().Master().Cache().SpaceByCache(this.Ctx.GetContext(), this.db, this.space)
	if err != nil {
		return err
	}

	if space.Engine.MetricType == "L2" { //if has sort it will err
		sortOrder = sortorder.SortOrder{&sortorder.SortScore{Desc: false}}
	}

	if len(dest) != len(src) {
		log.Error("dest length:[%d] not equal src length:[%d]", len(dest), len(src))
	}

	if log.IsDebugEnabled() {
		log.Debug("dest length:[%d] , src length:[%d]", len(dest), len(src))
	}

	if len(dest) <= len(src) {
		for index := range dest {
			err := dest[index].Merge(src[index], sortOrder, req.From, *req.Size)
			if err != nil {
				return fmt.Errorf("merge err [%s]")
			}
		}
	} else {
		for index := range src {
			err := dest[index].Merge(src[0], sortOrder, req.From, *req.Size)
			if err != nil {
				return fmt.Errorf("merge err [%s]")
			}
		}
	}

	return nil

}

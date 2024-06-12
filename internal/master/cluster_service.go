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

package master

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/pkg/errors"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/v3/internal/client"
	"github.com/vearch/vearch/v3/internal/config"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/pkg/errutil"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/pkg/number"
	"github.com/vearch/vearch/v3/internal/pkg/vjson"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
	"github.com/vearch/vearch/v3/internal/ps/engine/mapping"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// masterService is used for master administrator purpose.It should not be used by router and partition server program
type masterService struct {
	*client.Client
}

func newMasterService(client *client.Client) (*masterService, error) {
	return &masterService{client}, nil
}

// registerServerService find nodeId partitions
func (ms *masterService) registerServerService(ctx context.Context, ip string, nodeID entity.NodeID) (*entity.Server, error) {
	server := &entity.Server{Ip: ip}

	spaces, err := ms.Master().QuerySpacesByKey(ctx, entity.PrefixSpace)
	if err != nil {
		return nil, err
	}

	for _, s := range spaces {
		for _, p := range s.Partitions {
			for _, id := range p.Replicas {
				if nodeID == id {
					server.PartitionIds = append(server.PartitionIds, p.Id)
					server.Spaces = append(server.Spaces, s)
					break
				}
			}
		}
	}

	return server, nil
}

// registerPartitionService partition/[id]:[body]
func (ms *masterService) registerPartitionService(ctx context.Context, partition *entity.Partition) error {
	log.Info("register parttion:[%d] ", partition.Id)
	marshal, err := vjson.Marshal(partition)
	if err != nil {
		return err
	}
	return ms.Master().Put(ctx, entity.PartitionKey(partition.Id), marshal)
}

// createDBService three keys "db/id/[dbId]:[dbName]" ,"db/name/[dbName]:[dbId]" ,"db/body/[dbId]:[dbBody]"
func (ms *masterService) createDBService(ctx context.Context, db *entity.DB) (err error) {
	if ms.Master().Client().Master().Config().Global.LimitedDBNum {
		_, bytesArr, err := ms.Master().PrefixScan(ctx, entity.PrefixDataBaseBody)
		if err != nil {
			return err
		}
		if len(bytesArr) >= 1 {
			return fmt.Errorf("db num is limited to one and already have one db exists")
		}
	}

	//validate name has in db is in return err
	if err = db.Validate(); err != nil {
		return err
	}

	if err = ms.validatePS(ctx, db.Ps); err != nil {
		return err
	}

	//generate a new db id
	db.Id, err = ms.Master().NewIDGenerate(ctx, entity.DBIdSequence, 1, 5*time.Second)
	if err != nil {
		return err
	}

	err = ms.Master().STM(context.Background(), func(stm concurrency.STM) error {
		idKey, nameKey, bodyKey := ms.Master().DBKeys(db.Id, db.Name)

		if stm.Get(nameKey) != "" {
			return fmt.Errorf("dbname %s is exists", db.Name)
		}

		if stm.Get(idKey) != "" {
			return fmt.Errorf("dbID %d is exists", db.Id)
		}
		value, err := vjson.Marshal(db)
		if err != nil {
			return err
		}
		stm.Put(nameKey, cast.ToString(db.Id))
		stm.Put(idKey, db.Name)
		stm.Put(bodyKey, string(value))
		return nil
	})
	return err
}

func (ms *masterService) validatePS(ctx context.Context, psList []string) error {
	if len(psList) < 1 {
		return nil
	}
	servers, err := ms.Master().QueryServers(ctx)
	if err != nil {
		return err
	}
	for _, ps := range psList {
		flag := false
		for _, server := range servers {
			if server.Ip == ps {
				flag = true
				if !client.IsLive(server.RpcAddr()) {
					return fmt.Errorf("server:[%s] can not connection", ps)
				}
				break
			}
		}
		if !flag {
			return fmt.Errorf("server:[%s] not found in cluster", ps)
		}
	}
	return nil
}

func (ms *masterService) deleteDBService(ctx context.Context, dbstr string) (err error) {
	db, err := ms.queryDBService(ctx, dbstr)
	if err != nil {
		return err
	}
	//it will lock cluster
	mutex := ms.Master().NewLock(ctx, entity.PrefixLockCluster, time.Second*300)
	if err = mutex.Lock(); err != nil {
		return err
	}
	defer func() {
		if err := mutex.Unlock(); err != nil {
			log.Error("unlock space err %s", err)
		}
	}()

	spaces, err := ms.Master().QuerySpaces(ctx, db.Id)
	if err != nil {
		return err
	}

	if len(spaces) > 0 {
		return vearchpb.NewError(vearchpb.ErrorEnum_DB_NOT_EMPTY, nil)
	}

	err = ms.Master().STM(context.Background(),
		func(stm concurrency.STM) error {
			idKey, nameKey, bodyKey := ms.Master().DBKeys(db.Id, db.Name)
			stm.Del(idKey)
			stm.Del(nameKey)
			stm.Del(bodyKey)
			return nil
		})

	if err != nil {
		return err
	}

	return nil
}

func (ms *masterService) updateDBIpList(ctx context.Context, dbModify *entity.DBModify) (db *entity.DB, err error) {
	// process painc
	defer errutil.CatchError(&err)
	var id int64
	db = &entity.DB{}
	if number.IsNum(dbModify.DbName) {
		id = cast.ToInt64(dbModify.DbName)
	} else if id, err = ms.Master().QueryDBName2Id(ctx, dbModify.DbName); err != nil {
		return nil, err
	}
	bs, err := ms.Master().Get(ctx, entity.DBKeyBody(id))
	errutil.ThrowError(err)
	if bs == nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_DB_NOT_EXIST, nil)
	}
	err = vjson.Unmarshal(bs, db)
	errutil.ThrowError(err)
	if dbModify.Method == proto.ConfRemoveNode {
		ps := make([]string, 0, len(db.Ps))
		for _, ip := range db.Ps {
			if ip != dbModify.IPAddr {
				ps = append(ps, ip)
			}
		}
		db.Ps = ps
	} else if dbModify.Method == proto.ConfAddNode {
		exist := false
		for _, ip := range db.Ps {
			if ip == dbModify.IPAddr {
				exist = true
				break
			}
		}
		if !exist {
			db.Ps = append(db.Ps, dbModify.IPAddr)
		}
	} else {
		err = fmt.Errorf("method support addIP:[%d] removeIP:[%d] not support:[%d]",
			proto.ConfAddNode, proto.ConfRemoveNode, dbModify.Method)
		errutil.ThrowError(err)
	}
	log.Debug("db info is %v", db)
	_, _, bodyKey := ms.Master().DBKeys(db.Id, db.Name)
	value, err := vjson.Marshal(db)
	errutil.ThrowError(err)
	err = ms.Client.Master().Put(ctx, bodyKey, value)
	return db, err
}

func (ms *masterService) queryDBService(ctx context.Context, dbstr string) (db *entity.DB, err error) {
	var id int64
	db = &entity.DB{}
	if number.IsNum(dbstr) {
		id = cast.ToInt64(dbstr)
	} else if id, err = ms.Master().QueryDBName2Id(ctx, dbstr); err != nil {
		return nil, err
	}

	bs, err := ms.Master().Get(ctx, entity.DBKeyBody(id))

	if err != nil {
		return nil, err
	}

	if bs == nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_DB_NOT_EXIST, nil)
	}

	if err := vjson.Unmarshal(bs, db); err != nil {
		return nil, err
	} else {
		return db, nil
	}
}

// server/[serverAddr]:[serverBody]
// spaceKeys "space/[dbId]/[spaceId]:[spaceBody]"
func (ms *masterService) createSpaceService(ctx context.Context, dbName string, space *entity.Space) (err error) {
	if space.DBId, err = ms.Master().QueryDBName2Id(ctx, dbName); err != nil {
		log.Error("Failed When finding DbId according DbName:%v,And the Error is:%v", dbName, err)
		return err
	}

	// to validate schema
	_, err = mapping.SchemaMap(space.Fields)
	if err != nil {
		log.Error("master service createSpaceService error: %v", err)
		return err
	}

	// it will lock cluster to create space
	mutex := ms.Master().NewLock(ctx, entity.LockSpaceKey(dbName, spaceName), time.Second*300)
	if err = mutex.Lock(); err != nil {
		return err
	}
	defer func() {
		if err := mutex.Unlock(); err != nil {
			log.Error("unlock space err:[%s]", err.Error())
		}
	}()

	// spaces is existed
	if _, err := ms.Master().QuerySpaceByName(ctx, space.DBId, space.Name); err != nil {
		vErr := vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, err)
		if vErr.GetError().Code != vearchpb.ErrorEnum_SPACE_NOT_EXIST {
			return vErr
		}
	} else {
		return vearchpb.NewError(vearchpb.ErrorEnum_SPACE_EXIST, nil)
	}

	log.Info("create space, db: %s, spaceName: %s, space :[%s]", dbName, space.Name, vjson.ToJsonString(space))

	// find all servers for create space
	servers, err := ms.Master().QueryServers(ctx)
	if err != nil {
		return err
	}

	//generate space id
	spaceID, err := ms.Master().NewIDGenerate(ctx, entity.SpaceIdSequence, 1, 5*time.Second)
	if err != nil {
		return err
	}
	space.Id = spaceID

	width := math.MaxUint32 / space.PartitionNum
	for i := 0; i < space.PartitionNum; i++ {
		partitionID, err := ms.Master().NewIDGenerate(ctx, entity.PartitionIdSequence, 1, 5*time.Second)

		if err != nil {
			return err
		}

		space.Partitions = append(space.Partitions, &entity.Partition{
			Id:      entity.PartitionID(partitionID),
			SpaceId: space.Id,
			DBId:    space.DBId,
			Slot:    entity.SlotID(i * width),
		})
	}

	serverPartitions, err := ms.filterAndSortServer(ctx, space, servers)
	if err != nil {
		return err
	}

	if int(space.ReplicaNum) > len(serverPartitions) {
		return fmt.Errorf("not enough PS , need replica %d but only has %d",
			int(space.ReplicaNum), len(serverPartitions))
	}

	bFlase := false
	space.Enabled = &bFlase
	defer func() {
		if !(*space.Enabled) { // if space is still not enabled , so to remove it
			if e := ms.Master().Delete(context.Background(), entity.SpaceKey(space.DBId, space.Id)); e != nil {
				log.Error("to delete space err :%s", e.Error())
			}
		}
	}()

	spaceProperties, err := entity.UnmarshalPropertyJSON(space.Fields)
	if err != nil {
		return err
	}

	space.SpaceProperties = spaceProperties
	for _, f := range spaceProperties {
		if f.FieldType == vearchpb.FieldType_VECTOR && f.Index != nil {
			space.Index = f.Index
		}
	}

	marshal, err := vjson.Marshal(space)
	if err != nil {
		return err
	}
	err = ms.Master().Create(ctx, entity.SpaceKey(space.DBId, space.Id), marshal)
	if err != nil {
		return err
	}

	//peak servers for space
	var paddrs [][]string
	for i := 0; i < len(space.Partitions); i++ {
		if addrs, err := ms.generatePartitionsInfo(servers, serverPartitions, space.ReplicaNum, space.Partitions[i]); err != nil {
			return err
		} else {
			paddrs = append(paddrs, addrs)
		}
	}

	var errChain = make(chan error, 1)
	// send create space to every space
	for i := 0; i < len(space.Partitions); i++ {
		go func(addrs []string, partition *entity.Partition) {
			//send request for all server
			defer func() {
				if r := recover(); r != nil {
					err := vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("create partition err: %v ", r))
					errChain <- err
					log.Error(err.Error())
				}
			}()
			for _, addr := range addrs {
				if err := client.CreatePartition(addr, space, partition.Id); err != nil {
					err := vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("create partition err: %s ", err.Error()))
					errChain <- err
					log.Error(err.Error())
				}
			}
		}(paddrs[i], space.Partitions[i])
	}

	// check all partition is ok
	for i := 0; i < len(space.Partitions); i++ {
		v := 0
		for {
			v++
			select {
			case err := <-errChain:
				return err
			case <-ctx.Done():
				return vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("create space has error"))
			default:
			}

			partition, err := ms.Master().QueryPartition(ctx, space.Partitions[i].Id)
			if v%5 == 0 {
				log.Debug("check the partition:%d status ", space.Partitions[i].Id)
			}
			if err != nil && vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, err).GetError().Code != vearchpb.ErrorEnum_PARTITION_NOT_EXIST {
				return err
			}
			if partition != nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	bTrue := true
	space.Enabled = &bTrue

	//update version
	err = ms.updateSpace(ctx, space)
	if err != nil {
		bFalse := false
		space.Enabled = &bFalse
		return err
	}

	return nil
}

// create partitions for space create
func (ms *masterService) generatePartitionsInfo(servers []*entity.Server, serverPartitions map[int]int, replicaNum uint8, partition *entity.Partition) ([]string, error) {
	address := make([]string, 0, replicaNum)
	partition.Replicas = make([]entity.NodeID, 0, replicaNum)

	kvList := make([]struct {
		index  int
		length int
	}, 0, len(serverPartitions))

	for k, v := range serverPartitions {
		kvList = append(kvList, struct {
			index  int
			length int
		}{index: k, length: v})
	}

	sort.Slice(kvList, func(i, j int) bool {
		return kvList[i].length < kvList[j].length
	})

	//find addr for all servers
	for _, kv := range kvList {
		addr := servers[kv.index].RpcAddr()
		ID := servers[kv.index].ID

		if !client.IsLive(addr) {
			serverPartitions[kv.index] = kv.length
			continue
		}
		serverPartitions[kv.index] = serverPartitions[kv.index] + 1
		address = append(address, addr)
		partition.Replicas = append(partition.Replicas, ID)

		replicaNum--
		if replicaNum <= 0 {
			break
		}
	}

	if replicaNum > 0 {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_MASTER_PS_NOT_ENOUGH_SELECT, fmt.Errorf("need %d partition server but only get %d", len(partition.Replicas), len(address)))
	}

	return address, nil
}

func (ms *masterService) filterAndSortServer(ctx context.Context, space *entity.Space, servers []*entity.Server) (map[int]int, error) {
	db, err := ms.queryDBService(ctx, cast.ToString(space.DBId))
	if err != nil {
		return nil, err
	}

	var psMap map[string]bool
	if len(db.Ps) > 0 {
		psMap = make(map[string]bool)
		for _, ps := range db.Ps {
			psMap[ps] = true
		}
	}

	serverPartitions := make(map[int]int)

	spaces, err := ms.Master().QuerySpacesByKey(ctx, entity.PrefixSpace)
	if err != nil {
		return nil, err
	}

	serverIndex := make(map[entity.NodeID]int)

	if psMap == nil { //means only use public
		for i, s := range servers {
			// only resourceName equal can use
			if s.ResourceName != space.ResourceName {
				continue
			}
			if !s.Private {
				serverPartitions[i] = 0
				serverIndex[s.ID] = i
			}
		}
	} else { // only use define
		for i, s := range servers {
			// only resourceName equal can use
			if s.ResourceName != space.ResourceName {
				psMap[s.Ip] = false
				continue
			}
			if psMap[s.Ip] {
				serverPartitions[i] = 0
				serverIndex[s.ID] = i
			}
		}
	}

	for _, space := range spaces {
		for _, partition := range space.Partitions {
			for _, nodeID := range partition.Replicas {
				if index, ok := serverIndex[nodeID]; ok {
					serverPartitions[index] = serverPartitions[index] + 1
				}
			}
		}
	}

	return serverPartitions, nil
}

func (ms *masterService) deleteSpaceService(ctx context.Context, dbName string, spaceName string) error {
	//send delete to partition
	dbId, err := ms.Master().QueryDBName2Id(ctx, dbName)
	if err != nil {
		return err
	}

	space, err := ms.Master().QuerySpaceByName(ctx, dbId, spaceName)
	if err != nil {
		return err
	}
	if space == nil { //if zero it not exists
		return nil
	}

	mutex := ms.Master().NewLock(ctx, entity.LockSpaceKey(dbName, spaceName), time.Second*60)
	if err = mutex.Lock(); err != nil {
		return err
	}
	defer func() {
		if err := mutex.Unlock(); err != nil {
			log.Error("unlock space err:[%s]", err.Error())
		}
	}()
	//delete key
	err = ms.Master().Delete(ctx, entity.SpaceKey(dbId, space.Id))
	if err != nil {
		return err
	}

	// delete parition and partitionKey
	for _, p := range space.Partitions {
		for _, replica := range p.Replicas {
			if server, err := ms.Master().QueryServer(ctx, replica); err != nil {
				log.Error("query partition:[%d] for replica:[%s] has err:[%s]", p.Id, replica, err.Error())
			} else {
				if err := client.DeletePartition(server.RpcAddr(), p.Id); err != nil {
					log.Error("delete partition:[%d] for server:[%s] has err:[%s]", p.Id, server.RpcAddr(), err.Error())
				}
			}
		}
		err = ms.Master().Delete(ctx, entity.PartitionKey(p.Id))
		if err != nil {
			return err
		}
	}

	// delete alias
	if aliases, err := ms.queryAllAlias(ctx); err != nil {
		return err
	} else {
		for _, alias := range aliases {
			if alias.DbName == dbName && alias.SpaceName == spaceName {
				if err := ms.deleteAliasService(ctx, alias.Name); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (ms *masterService) queryDBs(ctx context.Context) ([]*entity.DB, error) {
	_, bytesArr, err := ms.Master().PrefixScan(ctx, entity.PrefixDataBaseBody)
	if err != nil {
		return nil, err
	}
	dbs := make([]*entity.DB, 0, len(bytesArr))
	for _, bs := range bytesArr {
		db := &entity.DB{}
		if err := vjson.Unmarshal(bs, db); err != nil {
			log.Error("decode db err: %s,and the bs is:%s", err.Error(), string(bs))
			continue
		}
		dbs = append(dbs, db)
	}

	return dbs, err
}

func (ms *masterService) describeSpaceService(ctx context.Context, space *entity.Space, spaceInfo *entity.SpaceInfo, detail_info bool) error {
	spaceStatus := 0
	color := []string{"green", "yellow", "red"}
	for _, spacePartition := range space.Partitions {
		p, err := ms.Master().QueryPartition(ctx, spacePartition.Id)
		if err != nil {
			*(spaceInfo.Errors) = append(*(spaceInfo.Errors), fmt.Sprintf("partition:[%d] not found in space: [%s]", spacePartition.Id, spaceName))
			continue
		}

		pStatus := 0

		nodeID := p.LeaderID
		if nodeID == 0 {
			*(spaceInfo.Errors) = append(*(spaceInfo.Errors), fmt.Sprintf("partition:[%d] no leader in space: [%s]", spacePartition.Id, spaceName))
			pStatus = 2
			nodeID = p.Replicas[0]
		}

		server, err := ms.Master().QueryServer(ctx, nodeID)
		if err != nil {
			*(spaceInfo.Errors) = append(*(spaceInfo.Errors), fmt.Sprintf("server:[%d] not found in space: [%s] , partition:[%d]", nodeID, spaceName, spacePartition.Id))
			pStatus = 2
			continue
		}

		partitionInfo, err := client.PartitionInfo(server.RpcAddr(), p.Id, detail_info)
		if err != nil {
			*(spaceInfo.Errors) = append(*(spaceInfo.Errors), fmt.Sprintf("query space:[%s] server:[%d] partition:[%d] info err :[%s]", spaceName, nodeID, spacePartition.Id, err.Error()))
			partitionInfo = &entity.PartitionInfo{}
			pStatus = 2
		} else {
			if len(partitionInfo.Unreachable) > 0 {
				pStatus = 1
			}
		}

		replicasStatus := make(map[entity.NodeID]string)
		for nodeID, status := range p.ReStatusMap {
			if status == entity.ReplicasOK {
				replicasStatus[nodeID] = "ReplicasOK"
			} else {
				replicasStatus[nodeID] = "ReplicasNotReady"
			}
		}

		//this must from space.Partitions
		partitionInfo.PartitionID = spacePartition.Id
		partitionInfo.Color = color[pStatus]
		partitionInfo.ReplicaNum = len(p.Replicas)
		partitionInfo.Ip = server.Ip
		partitionInfo.NodeID = server.ID
		partitionInfo.RepStatus = replicasStatus

		spaceInfo.Partitions = append(spaceInfo.Partitions, partitionInfo)

		if pStatus > spaceStatus {
			spaceStatus = pStatus
		}
	}
	docNum := uint64(0)
	for _, p := range spaceInfo.Partitions {
		docNum += cast.ToUint64(p.DocNum)
	}
	spaceInfo.Status = color[spaceStatus]
	spaceInfo.DocNum = docNum
	return nil
}

// createAliasService keys "/alias/alias_name:alias"
func (ms *masterService) createAliasService(ctx context.Context, alias *entity.Alias) (err error) {
	//validate name
	if err = alias.Validate(); err != nil {
		return err
	}
	mutex := ms.Master().NewLock(ctx, entity.LockAliasKey(alias.Name), time.Second*30)
	if err = mutex.Lock(); err != nil {
		return err
	}
	defer func() {
		if err := mutex.Unlock(); err != nil {
			log.Error("unlock lock for create alias err %s", err)
		}
	}()
	err = ms.Master().STM(context.Background(), func(stm concurrency.STM) error {
		aliasKey := entity.AliasKey(alias.Name)

		value := stm.Get(aliasKey)
		if value != "" {
			return vearchpb.NewError(vearchpb.ErrorEnum_ALIAS_EXIST, nil)
		}
		marshal, err := vjson.Marshal(alias)
		if err != nil {
			return err
		}
		stm.Put(aliasKey, string(marshal))
		return nil
	})
	return err
}

func (ms *masterService) deleteAliasService(ctx context.Context, alias_name string) (err error) {
	alias, err := ms.queryAliasService(ctx, alias_name)
	if err != nil {
		return err
	}
	//it will lock cluster
	mutex := ms.Master().NewLock(ctx, entity.LockAliasKey(alias_name), time.Second*30)
	if err = mutex.Lock(); err != nil {
		return err
	}
	defer func() {
		if err := mutex.Unlock(); err != nil {
			log.Error("unlock lock for delete alias err %s", err)
		}
	}()

	err = ms.Master().STM(context.Background(),
		func(stm concurrency.STM) error {
			stm.Del(entity.AliasKey(alias.Name))
			return nil
		})

	if err != nil {
		return err
	}

	return nil
}

func (ms *masterService) updateAliasService(ctx context.Context, alias *entity.Alias) (err error) {
	bs, err := ms.Master().Get(ctx, entity.AliasKey(alias.Name))
	if err != nil {
		return err
	}
	if bs == nil {
		return vearchpb.NewError(vearchpb.ErrorEnum_ALIAS_NOT_EXIST, nil)
	}

	//it will lock cluster
	mutex := ms.Master().NewLock(ctx, entity.LockAliasKey(alias.Name), time.Second*30)
	if err = mutex.Lock(); err != nil {
		return err
	}
	defer func() {
		if err := mutex.Unlock(); err != nil {
			log.Error("unlock lock for update alias err %s", err)
		}
	}()
	err = ms.Master().STM(context.Background(), func(stm concurrency.STM) error {
		marshal, err := vjson.Marshal(alias)
		if err != nil {
			return err
		}
		stm.Put(entity.AliasKey(alias.Name), string(marshal))
		return nil
	})
	return nil
}

func (ms *masterService) queryAllAlias(ctx context.Context) ([]*entity.Alias, error) {
	_, values, err := ms.Master().PrefixScan(ctx, entity.PrefixAlias)
	if err != nil {
		return nil, err
	}
	allAlias := make([]*entity.Alias, 0, len(values))
	for _, value := range values {
		if value == nil {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_ALIAS_NOT_EXIST, nil)
		}
		alias := &entity.Alias{}
		err = vjson.Unmarshal(value, alias)
		if err != nil {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("get alias:%s value:%s, err:%s", alias.Name, string(value), err.Error()))
		}
		allAlias = append(allAlias, alias)
	}

	return allAlias, err
}

func (ms *masterService) queryAliasService(ctx context.Context, alias_name string) (alias *entity.Alias, err error) {
	alias = &entity.Alias{Name: alias_name}

	bs, err := ms.Master().Get(ctx, entity.AliasKey(alias_name))

	if err != nil {
		return nil, err
	}

	if bs == nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_ALIAS_NOT_EXIST, nil)
	}

	err = vjson.Unmarshal(bs, alias)
	if err != nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("get alias:%s value:%s, err:%s", alias.Name, string(bs), err.Error()))
	}
	return alias, nil
}

func (ms *masterService) GetEngineCfg(ctx context.Context, dbName, spaceName string) (cfg *entity.EngineCfg, err error) {
	defer errutil.CatchError(&err)
	// get space info
	dbId, err := ms.Master().QueryDBName2Id(ctx, dbName)
	if err != nil {
		errutil.ThrowError(err)
	}

	space, err := ms.Master().QuerySpaceByName(ctx, dbId, spaceName)
	if err != nil {
		errutil.ThrowError(err)
	}
	// invoke all space nodeID
	if space != nil && space.Partitions != nil {
		for _, partition := range space.Partitions {
			// get all replicas nodeID
			if partition.Replicas != nil {
				for _, nodeID := range partition.Replicas {
					server, err := ms.Master().QueryServer(ctx, nodeID)
					errutil.ThrowError(err)
					// send rpc query
					log.Debug("invoke nodeID [%+v],address [%+v]", partition.Id, server.RpcAddr())
					cfg, err = client.GetEngineCfg(server.RpcAddr(), partition.Id)
					errutil.ThrowError(err)
					if err == nil {
						return cfg, nil
					}
				}
			}
		}
	}
	return nil, nil
}

func (ms *masterService) ModifyEngineCfg(ctx context.Context, dbName,
	spaceName string, cacheCfg *entity.EngineCfg) (err error) {
	defer errutil.CatchError(&err)
	// get space info
	dbId, err := ms.Master().QueryDBName2Id(ctx, dbName)
	if err != nil {
		errutil.ThrowError(err)
	}

	space, err := ms.Master().QuerySpaceByName(ctx, dbId, spaceName)
	if err != nil {
		errutil.ThrowError(err)
	}
	// invoke all space nodeID
	if space != nil && space.Partitions != nil {
		for _, partition := range space.Partitions {
			// get all replicas nodeID
			if partition.Replicas != nil {
				for _, nodeID := range partition.Replicas {
					server, err := ms.Master().QueryServer(ctx, nodeID)
					errutil.ThrowError(err)
					// send rpc query
					log.Debug("invoke nodeID [%+v],address [%+v]", partition.Id, server.RpcAddr())
					err = client.UpdateEngineCfg(server.RpcAddr(), cacheCfg, partition.Id)
					errutil.ThrowError(err)
				}
			}
		}
	}
	return nil
}

func (ms *masterService) updateSpaceService(ctx context.Context, dbName, spaceName string, temp *entity.Space) (*entity.Space, error) {
	// it will lock cluster ,to create space
	mutex := ms.Master().NewLock(ctx, entity.LockSpaceKey(dbName, spaceName), time.Second*300)
	if err := mutex.Lock(); err != nil {
		return nil, err
	}
	defer func() {
		if err := mutex.Unlock(); err != nil {
			log.Error("failed to unlock space,the Error is:%v ", err)
		}
	}()

	dbId, err := ms.Master().QueryDBName2Id(ctx, dbName)
	if err != nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("failed to find database id according database name:%v,the Error is:%v ", dbName, err))
	}

	space, err := ms.Master().QuerySpaceByName(ctx, dbId, spaceName)
	if err != nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("failed to find space according space name:%v,the Error is:%v ", spaceName, err))
	}

	if space == nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("can not found space by name : %s", spaceName))
	}

	buff := bytes.Buffer{}
	if temp.DBId != 0 && temp.DBId != space.DBId {
		buff.WriteString("db_id not same ")
	}

	if temp.PartitionNum != 0 && temp.PartitionNum != space.PartitionNum {
		buff.WriteString("partition_num can not change ")
	}
	if temp.ReplicaNum != 0 && temp.ReplicaNum != space.ReplicaNum {
		buff.WriteString("replica_num can not change ")
	}
	if buff.String() != "" {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf(buff.String()))
	}

	if temp.Name != "" {
		space.Name = temp.Name
	}

	if temp.Enabled != nil {
		space.Enabled = temp.Enabled
	}

	if err := space.Validate(); err != nil {
		return nil, err
	}

	space.Version++
	space.Partitions = temp.Partitions

	if temp.Fields != nil && len(temp.Fields) > 0 {
		//parse old space
		oldFieldMap, err := mapping.SchemaMap(space.Fields)
		if err != nil {
			return nil, err
		}

		//parse new space
		newFieldMap, err := mapping.SchemaMap(temp.Fields)
		if err != nil {
			return nil, err
		}

		for k, v := range oldFieldMap {
			if fm, ok := newFieldMap[k]; ok {
				if !mapping.Equals(v, fm) {
					return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("not equals by field:[%s] old[%v] new[%v]", k, v, fm))
				}
				delete(newFieldMap, k)
			}
		}

		if len(newFieldMap) > 0 {
			log.Info("change schema for space: %s , change fields : %d , value is :[%s]", space.Name, len(newFieldMap), string(temp.Fields))

			schema, err := mapping.MergeSchema(space.Fields, temp.Fields)
			if err != nil {
				return nil, err
			}

			space.Fields = schema
		}

	}

	// notify all partitions
	for _, p := range space.Partitions {
		partition, err := ms.Master().QueryPartition(ctx, p.Id)
		if err != nil {
			return nil, err
		}

		server, err := ms.Master().QueryServer(ctx, partition.LeaderID)
		if err != nil {
			return nil, err
		}

		if !client.IsLive(server.RpcAddr()) {
			return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_IS_CLOSED, fmt.Errorf("partition %s is shutdown", server.RpcAddr()))
		}
	}

	for _, p := range space.Partitions {
		partition, err := ms.Master().QueryPartition(ctx, p.Id)
		if err != nil {
			return nil, err
		}

		server, err := ms.Master().QueryServer(ctx, partition.LeaderID)
		if err != nil {
			return nil, err
		}

		log.Debug("update partition server is [%+v],space is [%+v], pid is [%+v]",
			server, space, p.Id)

		if err := client.UpdatePartition(server.RpcAddr(), space, p.Id); err != nil {
			log.Debug("UpdatePartition err is [%v]", err)
			return nil, err
		}
	}

	log.Debug("update space  is [%+v]", space)
	space.Version--
	if err := ms.updateSpace(ctx, space); err != nil {
		return nil, err
	} else {
		return space, nil
	}
}

func (ms *masterService) updateSpace(ctx context.Context, space *entity.Space) error {
	space.Version++
	space.PartitionNum = len(space.Partitions)
	marshal, err := vjson.Marshal(space)
	if err != nil {
		return err
	}
	if err = ms.Master().Update(ctx, entity.SpaceKey(space.DBId, space.Id), marshal); err != nil {
		return err
	}

	return nil
}

func (ms *masterService) BackupSpace(ctx context.Context, dbName, spaceName string, backup *entity.BackupSpace) (err error) {
	defer errutil.CatchError(&err)
	// get space info
	dbId, err := ms.Master().QueryDBName2Id(ctx, dbName)
	if err != nil {
		errutil.ThrowError(err)
	}

	space, err := ms.Master().QuerySpaceByName(ctx, dbId, spaceName)
	if err != nil {
		errutil.ThrowError(err)
	}
	if space == nil || space.Partitions == nil {
		return nil
	}
	// invoke all space nodeID
	for _, partition := range space.Partitions {
		if partition.Replicas != nil {
			for _, nodeID := range partition.Replicas {
				log.Debug("nodeID is [%+v],partition is [%+v], [%+v]", nodeID, partition.Id, partition.LeaderID)
				if nodeID != partition.LeaderID {
					continue
				}
				server, err := ms.Master().QueryServer(ctx, nodeID)
				errutil.ThrowError(err)
				log.Debug("invoke nodeID [%+v],address [%+v]", partition.Id, server.RpcAddr())
				err = client.BackupSpace(server.RpcAddr(), backup, partition.Id)
				errutil.ThrowError(err)
			}
			if len(partition.Replicas) == 1 && partition.LeaderID == 0 {
				server, err := ms.Master().QueryServer(ctx, partition.Replicas[0])
				errutil.ThrowError(err)
				log.Debug("invoke nodeID [%+v],address [%+v]", partition.Id, server.RpcAddr())
				err = client.BackupSpace(server.RpcAddr(), backup, partition.Id)
				errutil.ThrowError(err)
			}
		}
	}
	return nil
}

func (ms *masterService) updateSpaceResourceService(ctx context.Context, spaceResource *entity.SpaceResource) (*entity.Space, error) {
	// it will lock cluster, to create space
	mutex := ms.Master().NewLock(ctx, entity.LockSpaceKey(spaceResource.DbName, spaceResource.SpaceName), time.Second*300)
	if err := mutex.Lock(); err != nil {
		return nil, err
	}
	defer func() {
		if err := mutex.Unlock(); err != nil {
			log.Error("failed to unlock space, the Error is:%v ", err)
		}
	}()

	dbId, err := ms.Master().QueryDBName2Id(ctx, spaceResource.DbName)
	if err != nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("failed to find database id according database name:%v,the Error is:%v ", spaceResource.DbName, err))
	}

	space, err := ms.Master().QuerySpaceByName(ctx, dbId, spaceResource.SpaceName)
	if err != nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("failed to find space according space name:%v,the Error is:%v ", spaceResource.SpaceName, err))
	}

	if space == nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("can not found space by name : %s", spaceResource.SpaceName))
	}

	// now only support update partition num
	if space.PartitionNum >= spaceResource.PartitionNum {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("paritition_num: %d now should greater than origin space partition_num: %d", spaceResource.PartitionNum, space.PartitionNum))
	}

	partitions := make([]*entity.Partition, 0)
	for i := space.PartitionNum; i < spaceResource.PartitionNum; i++ {
		partitionID, err := ms.Master().NewIDGenerate(ctx, entity.PartitionIdSequence, 1, 5*time.Second)

		if err != nil {
			return nil, err
		}

		partitions = append(partitions, &entity.Partition{
			Id:      entity.PartitionID(partitionID),
			SpaceId: space.Id,
			DBId:    space.DBId,
		})
		log.Debug("updateSpacePartitionNum Generate partition id %d", partitionID)
	}

	// find all servers for update space partition
	servers, err := ms.Master().QueryServers(ctx)
	if err != nil {
		return nil, err
	}

	// will get all exist partition
	serverPartitions, err := ms.filterAndSortServer(ctx, space, servers)
	if err != nil {
		return nil, err
	}

	if int(space.ReplicaNum) > len(serverPartitions) {
		return nil, fmt.Errorf("not enough PS , need replica %d but only has %d",
			int(space.ReplicaNum), len(serverPartitions))
	}

	//peak servers for space
	var paddrs [][]string
	for i := 0; i < len(partitions); i++ {
		if addrs, err := ms.generatePartitionsInfo(servers, serverPartitions, space.ReplicaNum, partitions[i]); err != nil {
			return nil, err
		} else {
			paddrs = append(paddrs, addrs)
		}
	}

	log.Debug("updateSpacePartitionNum origin paritionNum %d, serverPartitions %v, paddrs %v", space.PartitionNum, serverPartitions, paddrs)

	// when create partition, new partition id will be stored in server partition cache
	space.PartitionNum = spaceResource.PartitionNum
	space.Partitions = append(space.Partitions, partitions...)

	var errChain = make(chan error, 1)
	// send create partition for new
	for i := 0; i < len(partitions); i++ {
		go func(addrs []string, partition *entity.Partition) {
			//send request for all server
			defer func() {
				if r := recover(); r != nil {
					err := vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("create partition err: %v ", r))
					errChain <- err
					log.Error(err.Error())
				}
			}()
			for _, addr := range addrs {
				if err := client.CreatePartition(addr, space, partition.Id); err != nil {
					err := vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("create partition err: %s ", err.Error()))
					errChain <- err
					log.Error(err.Error())
				}
			}
		}(paddrs[i], partitions[i])
	}

	// check all partition is ok
	for i := 0; i < len(partitions); i++ {
		times := 0
		for {
			times++
			select {
			case err := <-errChain:
				return nil, err
			case <-ctx.Done():
				return nil, vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("update space has error"))
			default:
			}

			partition, err := ms.Master().QueryPartition(ctx, partitions[i].Id)
			if times%5 == 0 {
				log.Debug("updateSpacePartitionNum check the partition:%d status", partitions[i].Id)
			}
			if err != nil && vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, err).GetError().Code != vearchpb.ErrorEnum_PARTITION_NOT_EXIST {
				return nil, err
			}
			if partition != nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

	//update space
	width := math.MaxUint32 / space.PartitionNum
	for i := 0; i < space.PartitionNum; i++ {
		space.Partitions[i].Slot = entity.SlotID(i * width)
	}
	log.Debug("updateSpacePartitionNum space version %d, partition_num %d", space.Version, space.PartitionNum)

	if err := ms.updateSpace(ctx, space); err != nil {
		return nil, err
	} else {
		return space, nil
	}
}

func (ms *masterService) ChangeMember(ctx context.Context, cm *entity.ChangeMember) error {
	partition, err := ms.Master().QueryPartition(ctx, cm.PartitionID)
	if err != nil {
		return err
	}

	space, err := ms.Master().QuerySpaceByID(ctx, partition.DBId, partition.SpaceId)
	if err != nil {
		return err
	}

	dbName, err := ms.Master().QueryDBId2Name(ctx, space.DBId)
	if err != nil {
		return err
	}

	spacePartition := space.GetPartition(cm.PartitionID)

	if cm.Method != 1 {
		for _, nodeID := range spacePartition.Replicas {
			if nodeID == cm.NodeID {
				return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("partition:[%d] already on server:[%d] in replicas:[%v]", cm.PartitionID, cm.NodeID, spacePartition.Replicas))
			}
		}
		spacePartition.Replicas = append(spacePartition.Replicas, cm.NodeID)
	} else {
		tempIDs := make([]entity.NodeID, 0, len(spacePartition.Replicas)-1)

		for _, id := range spacePartition.Replicas {
			if id != cm.NodeID {
				tempIDs = append(tempIDs, id)
			}
		}
		spacePartition.Replicas = tempIDs
	}

	masterNode, err := ms.Master().QueryServer(ctx, partition.LeaderID)
	if err != nil {
		return err
	}
	log.Info("masterNode is [%+v], cm is [%+v] ", masterNode, cm)

	var targetNode *entity.Server
	targetNode, err = ms.Master().QueryServer(ctx, cm.NodeID)
	if err != nil {
		if cm.Method == proto.ConfRemoveNode {
			// maybe node is crush
			targetNode = nil
		} else {
			return err
		}
	}
	log.Info("targetNode is [%+v], cm is [%+v] ", masterNode, cm)

	if !client.IsLive(masterNode.RpcAddr()) {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_SERVER_ERROR, fmt.Errorf("server:[%d] addr:[%s] can not connect ", cm.NodeID, masterNode.RpcAddr()))
	}

	if _, err := ms.updateSpaceService(ctx, dbName, space.Name, space); err != nil {
		return err
	}
	log.Info("cm is [%v] has update space ", cm)

	if cm.Method == proto.ConfAddNode && targetNode != nil {
		if err := client.CreatePartition(targetNode.RpcAddr(), space, cm.PartitionID); err != nil {
			return vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("create partiiton has err:[%s] addr:[%s]", err.Error(), targetNode.RpcAddr()))
		}
	} else if cm.Method == proto.ConfRemoveNode {

	} else {
		return vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("change member only support addNode:[%d] removeNode:[%d] not support:[%d]", proto.ConfAddNode, proto.ConfRemoveNode, cm.Method))
	}

	log.Info("execute change, master node info is [%+v]", masterNode)
	log.Info("change member info is [%+v]", cm)
	if err := client.ChangeMember(masterNode.RpcAddr(), cm); err != nil {
		return err
	}
	if cm.Method == proto.ConfRemoveNode && targetNode != nil && client.IsLive(targetNode.RpcAddr()) {
		if err := client.DeleteReplica(targetNode.RpcAddr(), cm.PartitionID); err != nil {
			return vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, fmt.Errorf("delete partiiton has err:[%s] addr:[%s]", err.Error(), targetNode.RpcAddr()))
		}
	}
	return nil
}

func (ms *masterService) ChangeMembers(ctx context.Context, cms *entity.ChangeMembers) error {
	for partitionId := range cms.PartitionIDs {
		cm := &entity.ChangeMember{
			PartitionID: uint32(partitionId),
			NodeID:      cms.NodeID,
			Method:      cms.Method,
		}
		if err := ms.ChangeMember(ctx, cm); err != nil {
			return err
		}
	}
	return nil
}

// recover fail node
func (ms *masterService) RecoverFailServer(ctx context.Context, rs *entity.RecoverFailServer) (e error) {
	// painc process
	defer errutil.CatchError(&e)
	// get failserver info
	targetFailServer := ms.Master().QueryServerByIPAddr(ctx, rs.FailNodeAddr)
	log.Debug("targetFailServer is %s ", targetFailServer)
	// get newserver info
	newServer := ms.Master().QueryServerByIPAddr(ctx, rs.NewNodeAddr)
	log.Debug("newServer is %s ", newServer)
	if newServer.ID > 0 && targetFailServer.ID > 0 {
		for _, pid := range targetFailServer.Node.PartitionIds {
			cm := &entity.ChangeMember{}
			cm.Method = proto.ConfAddNode
			cm.NodeID = newServer.ID
			cm.PartitionID = pid
			if e = ms.ChangeMember(ctx, cm); e != nil {
				info := fmt.Sprintf("ChangePartitionMember failed [%+v],err is %s ", cm, e.Error())
				log.Error(info)
				panic(fmt.Errorf(info))
			}
			log.Info("ChangePartitionMember [%+v] success,", cm)
		}
		// if success, remove from failServer
		ms.Master().TryRemoveFailServer(ctx, targetFailServer.Node)
	} else {
		return vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_SERVER_ERROR, fmt.Errorf("newServer or targetFailServer is nil"))
	}

	return e
}

// get servers belong this db
func (ms *masterService) DBServers(ctx context.Context, dbName string) (servers []*entity.Server, err error) {
	defer errutil.CatchError(&err)
	// get all servers
	servers, err = ms.Master().QueryServers(ctx)
	errutil.ThrowError(err)

	db, err := ms.queryDBService(ctx, dbName)
	if err != nil {
		return nil, err
	}
	// get private server
	if len(db.Ps) > 0 {
		privateServer := make([]*entity.Server, 0)
		for _, ps := range db.Ps {
			for _, s := range servers {
				if ps == s.Ip {
					privateServer = append(privateServer, s)
				}
			}
		}
		return privateServer, nil
	}
	return servers, nil
}

// change replicas, add or delete
func (ms *masterService) ChangeReplica(ctx context.Context, dbModify *entity.DBModify) (e error) {
	// painc process
	defer errutil.CatchError(&e)
	// query server
	servers, err := ms.DBServers(ctx, dbModify.DbName)
	errutil.ThrowError(err)
	// generate change servers
	dbID, err := ms.Master().QueryDBName2Id(ctx, dbModify.DbName)
	errutil.ThrowError(err)
	space, err := ms.Master().QuerySpaceByName(ctx, dbID, dbModify.SpaceName)
	errutil.ThrowError(err)
	if dbModify.Method == proto.ConfAddNode && (int(space.ReplicaNum)+1) > len(servers) {
		err := vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("ReplicaNum [%d] is exceed server size [%d]",
			int(space.ReplicaNum)+1, len(servers)))
		return err
	}
	// change space replicas of partition, add or delete one
	changeServer := make([]*entity.ChangeMember, 0)
	for _, partition := range space.Partitions {
		// sort servers，low to high
		sort.Slice(servers, func(i, j int) bool {
			return len(servers[i].PartitionIds) < len(servers[j].PartitionIds)
		})
		// change server
		for _, s := range servers {
			if dbModify.Method == proto.ConfAddNode {
				exist, _ := number.IsExistSlice(s.ID, partition.Replicas)
				if !exist {
					// server don't contain this partition, then create it
					cm := &entity.ChangeMember{PartitionID: partition.Id, NodeID: s.ID, Method: dbModify.Method}
					changeServer = append(changeServer, cm)
					s.PartitionIds = append(s.PartitionIds, partition.Id)
					break
				}
			} else if dbModify.Method == proto.ConfRemoveNode {
				exist, index := number.IsExistSlice(partition.Id, s.PartitionIds)
				if exist {
					// server contain this partition, then remove it
					cm := &entity.ChangeMember{PartitionID: partition.Id, NodeID: s.ID, Method: dbModify.Method}
					changeServer = append(changeServer, cm)
					s.PartitionIds = append(s.PartitionIds[:index], s.PartitionIds[index+1:]...)
					break
				}
			}
		}
	}
	log.Info("need change partition is [%+v] ", vjson.ToJsonString(changeServer))
	// sleep time
	sleepTime := config.Conf().PS.RaftHeartbeatInterval
	// change partition
	for _, cm := range changeServer {
		if e = ms.ChangeMember(ctx, cm); e != nil {
			info := fmt.Sprintf("change partition member [%+v] failed, err is %s ", cm, e)
			log.Error(info)
			panic(fmt.Errorf(info))
		}
		log.Info("change partition member [%+v] success ", cm)
		if dbModify.Method == proto.ConfRemoveNode {
			time.Sleep(time.Duration(sleepTime*10) * time.Millisecond)
			log.Info("remove partition sleep [%+d] Millisecond time", sleepTime)
		}
	}
	log.Info("all change partition member success")

	// update space ReplicaNum, it will lock cluster, to create space
	mutex := ms.Master().NewLock(ctx, entity.LockSpaceKey(dbModify.DbName, dbModify.SpaceName), time.Second*300)
	if _, err := mutex.TryLock(); err != nil {
		errutil.ThrowError(err)
	}
	defer func() {
		if err := mutex.Unlock(); err != nil {
			log.Error("failed to unlock space,the Error is:%v ", err)
		}
	}()
	space, err = ms.Master().QuerySpaceByName(ctx, dbID, dbModify.SpaceName)
	errutil.ThrowError(err)
	if dbModify.Method == proto.ConfAddNode {
		space.ReplicaNum = space.ReplicaNum + 1
	} else if dbModify.Method == proto.ConfRemoveNode {
		space.ReplicaNum = space.ReplicaNum - 1
	}
	err = ms.updateSpace(ctx, space)
	log.Info("updateSpace space [%+v] success", space)
	errutil.ThrowError(err)
	return e
}

func (ms *masterService) IsExistNode(ctx context.Context, id entity.NodeID, ip string) error {
	values, err := ms.Master().Get(ctx, entity.ServerKey(id))
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("look up key[%s] in etcd failed", entity.ServerKey(id)))
	}
	if values == nil {
		return nil
	}
	server := &entity.Server{}
	err = vjson.Unmarshal(values, server)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("parse key[%s] in etcd failed", entity.ServerKey(id)))
	}
	if server.Ip != ip {
		return errors.Errorf("node id[%d] has register on ip[%s]", server.ID, server.Ip)
	}
	return nil
}

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
	"context"
	"encoding/json"
	"fmt"
	"github.com/vearch/vearch/config"
	"sync"
	"time"

	"github.com/spf13/cast"
	"github.com/vearch/vearch/util/log"
	"github.com/vearch/vearch/master/store"
	"github.com/vearch/vearch/proto"
	. "github.com/vearch/vearch/proto/entity"
	"go.etcd.io/etcd/clientv3"
)

// masterClient is  used for router and partition server,not for master administrator. This client is mainly used to communicate with etcd directly,with out business logic
// if method has query , it not use cache
type masterClient struct {
	client *Client
	store.Store
	cfg      *config.Config
	once     sync.Once
	cliCache *clientCache
}

func (this *masterClient) Client() *Client {
	return this.client
}

func (this *masterClient) Cache() *clientCache {
	return this.cliCache
}

func (this *masterClient) FlushCacheJob(ctx context.Context) error {
	cliCache, err := newClientCache(ctx, this)
	if err != nil {
		return err
	}

	old := this.cliCache
	this.cliCache = cliCache
	if old != nil {
		old.stopCacheJob()
	}

	return nil
}

//set cached
func (client *masterClient) Stop() {
	if client.cliCache != nil {
		client.cliCache.stopCacheJob()
	}
}

// find name by id
func (client *masterClient) QueryDBId2Name(ctx context.Context, id int64) (string, error) {
	bytes, err := client.Get(ctx, DBKeyId(id))
	if err != nil {
		return "", err
	}
	if bytes == nil {
		return "", pkg.ErrMasterDbNotExists
	}
	return string(bytes), nil
}

func (client *masterClient) QueryDBName2Id(ctx context.Context, name string) (int64, error) {
	if bytes, err := client.Get(ctx, DBKeyName(name)); err != nil {
		return -1, err
	} else if bytes == nil {
		return -1, pkg.ErrMasterDbNotExists
	} else {
		return cast.ToInt64E(string(bytes))
	}
}

func (this *masterClient) QueryPartition(ctx context.Context, partitionId PartitionID) (*Partition, error) {
	bytes, err := this.Get(ctx, PartitionKey(partitionId))
	if err != nil {
		return nil, err
	}
	if bytes == nil {
		return nil, pkg.ErrPartitionNotExist
	}

	p := new(Partition)
	err = json.Unmarshal(bytes, p)
	return p, err
}

func (this *masterClient) QueryServer(ctx context.Context, id NodeID) (*Server, error) {
	bytes, err := this.Get(ctx, ServerKey(id))
	if err != nil {
		log.Error("QueryServer() error, can not connect master, nodeId:[%d], err:[%v]", id, err)
		return nil, err
	}
	if bytes == nil {
		log.Error("server can not find on master, maybe server is offline, nodeId:[%d]", id)
		return nil, pkg.ErrMasterPSNotExists
	}

	p := new(Server)
	if err = json.Unmarshal(bytes, p); err != nil {
		log.Error("server find on master, but json.Unmarshal(bytes, p) error, nodeId:[%d], bytes:[%s], err:[%v]", id, string(bytes), err)
		return nil, err
	}

	return p, err
}

func (this *masterClient) QueryUser(ctx context.Context, username string) (*User, error) {
	bytes, err := this.Get(ctx, UserKey(username))
	if err != nil {
		return nil, err
	}
	if bytes == nil {
		return nil, pkg.ErrMasterUseerNotExists
	}

	user := new(User)
	if err = json.Unmarshal(bytes, user); err != nil {
		return nil, err
	}

	return user, nil
}

func (this *masterClient) QueryUserByPassword(ctx context.Context, username, password string) (*User, error) {
	bytes, err := this.Get(ctx, UserKey(username))
	if err != nil {
		return nil, err
	}
	if bytes == nil {
		return nil, pkg.ErrMasterUseerNotExists
	}

	user := new(User)
	if err = json.Unmarshal(bytes, user); err != nil {
		return nil, err
	}

	if user.Password != password {
		return nil, pkg.ErrMasterAuthenticationFailed
	}

	return user, nil
}

func (this *masterClient) QueryServers(ctx context.Context) ([]*Server, error) {
	_, bytesArr, err := this.PrefixScan(ctx, PrefixServer)
	if err != nil {
		return nil, err
	}
	servers := make([]*Server, 0, len(bytesArr))
	for _, bs := range bytesArr {
		s := &Server{}
		if err := json.Unmarshal(bs, s); err != nil {
			log.Error("unmarshl space err: %s", err.Error())
			continue
		}
		servers = append(servers, s)
	}

	return servers, err
}

func (this *masterClient) QuerySpaces(ctx context.Context, dbId int64) ([]*Space, error) {
	return this.QuerySpacesByKey(ctx, fmt.Sprintf("%s%d/", PrefixSpace, dbId))
}

func (this *masterClient) QuerySpacesByKey(ctx context.Context, prefix string) ([]*Space, error) {

	_, bytesArr, err := this.PrefixScan(ctx, prefix)
	if err != nil {
		return nil, err
	}
	spaces := make([]*Space, 0, len(bytesArr))
	for _, bs := range bytesArr {
		s := &Space{}
		if err := json.Unmarshal(bs, s); err != nil {
			log.Error("unmarshl space err: %s", err.Error())
			continue
		}
		spaces = append(spaces, s)
	}

	return spaces, err
}

//QueryPartitions get all partitions from the etcd
func (this *masterClient) QueryPartitions(ctx context.Context) ([]*Partition, error) {
	_, bytesArr, err := this.PrefixScan(ctx, PrefixPartition)
	if err != nil {
		return nil, err
	}
	partitions := make([]*Partition, 0, len(bytesArr))
	for _, partitionBytes := range bytesArr {
		p := &Partition{}
		if err := json.Unmarshal(partitionBytes, p); err != nil {
			log.Error("unmarshl partition err: %s", err.Error())
			continue
		}
		partitions = append(partitions, p)
	}
	return partitions, err
}

func (this *masterClient) QuerySpaceById(ctx context.Context, dbId DBID, spaceId SpaceID) (*Space, error) {
	bytes, err := this.Store.Get(ctx, SpaceKey(dbId, spaceId))
	if err != nil {
		return nil, err
	}
	if bytes == nil {
		return nil, pkg.ErrMasterSpaceNotExists
	}
	space := &Space{}
	if err := json.Unmarshal(bytes, space); err != nil {
		return nil, err
	}
	return space, nil
}

func (this *masterClient) QuerySpaceByName(ctx context.Context, dbId int64, spaceName string) (*Space, error) {
	spaces, err := this.QuerySpaces(ctx, dbId)
	if err != nil {
		return nil, err
	}
	for _, s := range spaces {
		if s.Name == spaceName {
			return s, nil
		}
	}
	return nil, pkg.ErrMasterSpaceNotExists
}

func (this *masterClient) KeepAlive(ctx context.Context, server *Server) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	bytes, err := json.Marshal(server)
	if err != nil {
		return nil, err
	}
	return this.Store.KeepAlive(ctx, ServerKey(server.ID), bytes, time.Second*188)
}

func (this *masterClient) PutServerWithLeaseId(ctx context.Context, server *Server, leaseId clientv3.LeaseID) error {
	bytes, err := json.Marshal(server)
	if err != nil {
		return err
	}
	return this.Store.PutWithLeaseId(ctx, ServerKey(server.ID), bytes, time.Second*188, leaseId)
}

func (client *masterClient) DBKeys(id int64, name string) (idKey, nameKey, bodyKey string) {
	idKey = DBKeyId(id)
	nameKey = DBKeyName(name)
	bodyKey = DBKeyBody(id)
	return
}

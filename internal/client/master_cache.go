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
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/patrickmn/go-cache"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/internal/config"
	"github.com/vearch/vearch/internal/entity"
	"github.com/vearch/vearch/internal/pkg/cbjson"
	"github.com/vearch/vearch/internal/pkg/errutil"
	"github.com/vearch/vearch/internal/pkg/log"
	"github.com/vearch/vearch/internal/pkg/vearchlog"
	"github.com/vearch/vearch/internal/proto/vearchpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

const retryNum = 3
const retrySleepTime = 200 * time.Microsecond

var spaceCacheLock sync.Mutex
var (
	userReloadWorkder      sync.Map
	spaceReloadWorkder     sync.Map
	partitionReloadWorkder sync.Map
	serverReloadWorkder    sync.Map
)

type clientCache struct {
	sync.Map
	mc                                                               *masterClient
	cancel                                                           context.CancelFunc
	lock                                                             sync.Mutex
	userCache, spaceCache, spaceIDCache, partitionCache, serverCache *cache.Cache
}

func newClientCache(serverCtx context.Context, masterClient *masterClient) (*clientCache, error) {
	ctx, cancel := context.WithCancel(serverCtx)

	cc := &clientCache{
		mc:             masterClient,
		cancel:         cancel,
		userCache:      cache.New(cache.NoExpiration, cache.NoExpiration),
		spaceCache:     cache.New(cache.NoExpiration, cache.NoExpiration),
		spaceIDCache:   cache.New(cache.NoExpiration, cache.NoExpiration),
		partitionCache: cache.New(cache.NoExpiration, cache.NoExpiration),
		serverCache:    cache.New(cache.NoExpiration, cache.NoExpiration),
	}

	if err := cc.startCacheJob(ctx); err != nil {
		return nil, err
	}

	return cc, nil
}

// NewWatchServerCache watch ps server put and delete status
func NewWatchServerCache(serverCtx context.Context, cli *Client) error {
	ctx, cancel := context.WithCancel(serverCtx)

	cc := &clientCache{
		mc:          cli.Master(),
		cancel:      cancel,
		serverCache: cache.New(cache.NoExpiration, cache.NoExpiration),
	}

	err := cc.startWSJob(ctx)

	return err
}

func cachePartitionKey(space string, pid entity.PartitionID) string {
	return space + "/" + strconv.FormatInt(int64(pid), 10)
}

func cacheSpaceKey(db, space string) string {
	return db + "/" + space
}

func cacheServerKey(nodeID entity.NodeID) string {
	return cast.ToString(nodeID)
}

// find a user by cache
func (cliCache *clientCache) UserByCache(ctx context.Context, userName string) (*entity.User, error) {

	get, found := cliCache.userCache.Get(userName)
	if found {
		return get.(*entity.User), nil
	}

	_ = cliCache.reloadUserCache(ctx, false, userName)

	for i := 0; i < retryNum; i++ {
		time.Sleep(retrySleepTime)
		log.Debug("to find user by key:[%s] ", userName)
		if get, found = cliCache.spaceCache.Get(userName); found {
			return get.(*entity.User), nil
		}
	}

	return nil, fmt.Errorf("user:[%s] err:[%s]", userName, vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_NOT_EXIST, nil))
}

func (cliCache *clientCache) reloadUserCache(ctx context.Context, sync bool, userName string) error {
	fun := func() error {
		log.Info("to reload user:[%s]", userName)
		user, err := cliCache.mc.QueryUser(ctx, userName)
		if err != nil {
			return fmt.Errorf("can not found user by name:[%s] err:[%s]", userName, err.Error())
		}
		cliCache.userCache.Set(userName, user, cache.NoExpiration)
		return nil
	}

	if sync {
		return fun()
	}
	if _, ok := userReloadWorkder.LoadOrStore(userName, struct{}{}); !ok {
		go func() {
			defer userReloadWorkder.Delete(userName)
			vearchlog.FunIfNotNil(fun)
		}()
	}
	return nil
}

// find a space by db and space name , if not exist so query it from db
func (cliCache *clientCache) SpaceByCache(ctx context.Context, db, space string) (*entity.Space, error) {
	key := cacheSpaceKey(db, space)

	get, found := cliCache.spaceCache.Get(key)
	if found {
		return get.(*entity.Space), nil
	}

	err := cliCache.reloadSpaceCache(ctx, false, db, space)
	vearchlog.LogErrNotNil(err)

	if err != nil {
		return nil, fmt.Errorf("db:[%s] space:[%s] err:[%s]", db, space,
			vearchpb.NewError(vearchpb.ErrorEnum_SPACE_NOTEXISTS, nil))
	}

	for i := 0; i < retryNum; i++ {
		time.Sleep(retrySleepTime)
		log.Debug("to find space by key:[%s] ", key)
		if get, found = cliCache.spaceCache.Get(key); found {
			return get.(*entity.Space), nil
		}
	}

	return nil, fmt.Errorf("db:[%s] space:[%s] err:[%s]", db, space, vearchpb.NewError(vearchpb.ErrorEnum_SPACE_NOTEXISTS, nil))
}

func (cliCache *clientCache) reloadSpaceCache(ctx context.Context, sync bool, db string, spaceName string) error {
	key := cacheSpaceKey(db, spaceName)

	fun := func() error {
		log.Info("to reload db:[%s] space:[%s]", db, spaceName)

		dbID, err := cliCache.mc.QueryDBName2Id(ctx, db)
		if err != nil {
			return fmt.Errorf("can not found db by name:[%s] err:[%s]", db, err.Error())
		}

		space, err := cliCache.mc.QuerySpaceByName(ctx, dbID, spaceName)
		if err != nil {
			return fmt.Errorf("can not found db by name:[%s] err:[%s]", db, err.Error())
		}
		if space.ResourceName != config.Conf().Global.ResourceName {
			log.Info("space name [%s] resource name don't match [%s], [%s], reloadSpaceCache failed. ",
				space.Name, space.ResourceName, config.Conf().Global.ResourceName)
			return nil
		}
		spaceCacheLock.Lock()
		defer spaceCacheLock.Unlock()
		cliCache.spaceCache.Set(key, space, cache.NoExpiration)
		cliCache.spaceIDCache.Set(cast.ToString(space.Id), space, cache.NoExpiration)
		return nil
	}

	if sync {
		return fun()
	}
	if _, ok := spaceReloadWorkder.LoadOrStore(key, struct{}{}); ok {
		return nil
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				vearchlog.LogErrNotNil(fmt.Errorf(cast.ToString(r)))
			}
		}()
		if key == "" {
			return
		}
		defer spaceReloadWorkder.Delete(key)
		vearchlog.FunIfNotNil(fun)
	}()
	return nil
}

// partition/[spaceId]/[id]:[body]
func (cliCache *clientCache) PartitionByCache(ctx context.Context, spaceName string, pid entity.PartitionID) (*entity.Partition, error) {
	key := cachePartitionKey(spaceName, pid)
	get, found := cliCache.partitionCache.Get(key)
	if found {
		return get.(*entity.Partition), nil
	}

	_ = cliCache.reloadPartitionCache(ctx, false, spaceName, pid)

	for i := 0; i < retryNum; i++ {
		time.Sleep(retrySleepTime)
		log.Debug("to find partition by key:[%s] ", key)
		if get, found = cliCache.partitionCache.Get(key); found {
			return get.(*entity.Partition), nil
		}
	}

	return nil, fmt.Errorf("space:[%s] partition_id:[%d] err:[%s]", spaceName, pid, vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_NOT_EXIST, nil))
}

func (cliCache *clientCache) reloadPartitionCache(ctx context.Context, sync bool, spaceName string, pid entity.PartitionID) error {
	key := cachePartitionKey(spaceName, pid)

	fun := func() error {
		log.Info("to reload space:[%s] partition_id:[%d] ", spaceName, pid)

		c, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		partition, err := cliCache.mc.QueryPartition(c, pid)
		if err != nil {
			return fmt.Errorf("can not found db by space:[%s] partition_id:[%d] err:[%s]", spaceName, pid, err.Error())
		}

		cliCache.partitionCache.Set(key, partition, cache.NoExpiration)

		return nil
	}

	if sync {
		return fun()
	} else {
		if _, ok := partitionReloadWorkder.LoadOrStore(key, struct{}{}); ok {
			return nil
		}
		go func() {
			defer partitionReloadWorkder.Delete(key)
			vearchlog.FunIfNotNil(fun)
		}()
	}

	return nil
}

func (cliCache *clientCache) ServerByCache(ctx context.Context, id entity.NodeID) (*entity.Server, error) {
	key := cast.ToString(id)
	get, found := cliCache.serverCache.Get(key)
	if found {
		return get.(*entity.Server), nil
	}

	_ = cliCache.reloadServerCache(ctx, false, id)

	for i := 0; i < retryNum; i++ {
		time.Sleep(retrySleepTime)
		log.Debug("to find server by key:[%s] ", key)
		if get, found = cliCache.serverCache.Get(key); found {
			return get.(*entity.Server), nil
		}
	}

	return nil, fmt.Errorf("node_id:[%d] err:[%s]", id, vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_NOT_EXIST, nil))
}

func (cliCache *clientCache) reloadServerCache(ctx context.Context, sync bool, id entity.NodeID) error {
	key := cast.ToString(id)

	fun := func() error {
		log.Info("to reload server:[%d] ", id)

		c, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		server, err := cliCache.mc.QueryServer(c, id)
		if err != nil {
			return fmt.Errorf("can not found server node_id:[%d] err:[%s]", id, err.Error())
		}

		cliCache.serverCache.Set(key, server, cache.NoExpiration)

		return nil
	}

	if sync {
		return fun()
	}
	if _, ok := serverReloadWorkder.LoadOrStore(key, struct{}{}); ok {
		return nil
	}
	go func() {
		defer serverReloadWorkder.Delete(key)
		vearchlog.FunIfNotNil(fun)
	}()

	return nil
}

// the job will start watch server job
// if new server lose heart beat then will remove it from space meta
// if empty new node join into, will try to recover the last fail server
func (cliCache *clientCache) startWSJob(ctx context.Context) error {
	log.Debug("to start job to watch server")
	start := time.Now()
	// init server
	if err := cliCache.initServer(ctx); err != nil {
		return err
	}
	log.Debug("server info is %v", *cliCache.serverCache)
	serverJob := watcherJob{ctx: ctx, prefix: entity.PrefixServer, masterClient: cliCache.mc, cache: cliCache.serverCache}
	serverJob.put = serverJob.serverPut
	serverJob.delete = serverJob.serverDelete
	serverJob.start()
	log.Debug("watcher server job init ok use time %v", time.Since(start))
	return nil
}

// delete record
func (cliCache *clientCache) serverDelete(ctx context.Context, server *entity.Server) error {
	//mutex ensure only one master update the meta,the other just undate local cache
	mutex := cliCache.mc.Client().Master().NewLock(ctx, entity.ClusterWatchServerKey, time.Second*188)
	if getLock, err := mutex.TryLock(); getLock && err == nil {
		defer func() {
			if err := mutex.Unlock(); err != nil {
				log.Error("failed to unlock space,the Error is:%v ", err)
			}
		}()
		log.Debug("get LOCK success,process fail server %+v ", server)

		//get failServer info
		if len(server.PartitionIds) == 0 {
			// recover failServer
			// if the partition num of the newNode is empty, then recover data by it.
			if config.Conf().Global.AutoRecoverPs {
				err = cliCache.mc.RecoverByNewServer(ctx, server)
				if err != nil {
					log.Debug("auto recover is err %v,server is %+v", err, server)
				} else {
					log.Info("recover is success,server is %+v", server)
				}
			}
		} else {
			// if failserver recover,then remove record
			cliCache.mc.TryRemoveFailServer(ctx, server)
		}
	} else {
		log.Debug("get LOCK error,just update cache %+v ", server)
	}
	return nil
}

// record fail server
func (cliCache *clientCache) serverPut(ctx context.Context, server *entity.Server) error {
	//mutex ensure only one master update the meta,the other just undate local cache
	mutex := cliCache.mc.Client().Master().NewLock(ctx, entity.ClusterWatchServerKey, time.Second*188)
	if getLock, err := mutex.TryLock(); getLock && err == nil {
		defer func() {
			if err := mutex.Unlock(); err != nil {
				log.Error("failed to unlock space,the Error is:%v ", err)
			}
		}()
		log.Debug("get LOCK success,process fail server %+v ", server)

		//get failServer info
		if len(server.PartitionIds) == 0 {
			// recover failServer
			// if the partition num of the newNode is empty, then recover data by it.
			if config.Conf().Global.AutoRecoverPs {
				err = cliCache.mc.RecoverByNewServer(ctx, server)
				if err != nil {
					log.Debug("auto recover is err %v,server is %+v", err, server)
				} else {
					log.Info("recover is success,server is %+v", server)
				}
			}
		} else {
			// if failserver recover,then remove record
			cliCache.mc.TryRemoveFailServer(ctx, server)
		}
	} else {
		log.Debug("get LOCK error,just update cache %+v ", server)
	}
	return nil
}

// it will start cache
func (cliCache *clientCache) startCacheJob(ctx context.Context) error {
	log.Info("to start cache job begin")
	start := time.Now()

	//init user
	if err := cliCache.initUser(ctx); err != nil {
		return err
	}
	userJob := watcherJob{ctx: ctx, prefix: entity.PrefixUser, masterClient: cliCache.mc, cache: cliCache.userCache,
		put: func(value []byte) (err error) {
			user := &entity.User{}
			if err := cbjson.Unmarshal(value, user); err != nil {
				return fmt.Errorf("put event user cache err, can't unmarshal event value: %s , error: %s", string(value), err.Error())
			}
			cliCache.userCache.Set(entity.UserKey(user.Name), user, cache.NoExpiration)
			return nil
		},
		delete: func(key string) (err error) {
			userSplit := strings.Split(key, "/")
			username := userSplit[len(userSplit)-1]
			cliCache.userCache.Delete(username)
			return nil
		},
	}

	userJob.start()

	//init space
	if err := cliCache.initSpace(ctx); err != nil {
		return err
	}
	spaceJob := watcherJob{ctx: ctx, prefix: entity.PrefixSpace, masterClient: cliCache.mc, cache: cliCache.spaceCache,
		put: func(value []byte) (err error) {
			space := &entity.Space{}
			if err := cbjson.Unmarshal(value, space); err != nil {
				return err
			}
			if space.ResourceName != config.Conf().Global.ResourceName {
				log.Debug("space name [%s] resource name don't match [%s], [%s] add cache ignore.",
					space.Name, space.ResourceName, config.Conf().Global.ResourceName)
				return nil
			}
			dbName, err := cliCache.mc.QueryDBId2Name(ctx, space.DBId)
			if err != nil {
				return fmt.Errorf("change cache space err: %s , not found db content: %s", err.Error(), string(value))
			}
			key := cacheSpaceKey(dbName, space.Name)
			if oldValue, b := cliCache.spaceCache.Get(key); !b || space.Version > oldValue.(*entity.Space).Version {
				spaceCacheLock.Lock()
				cliCache.spaceCache.Set(key, space, cache.NoExpiration)
				cliCache.spaceIDCache.Set(cast.ToString(space.Id), space, cache.NoExpiration)
				log.Debug("space name [%s] , [%s], [%s] add to cache.",
					space.Name, space.ResourceName, config.Conf().Global.ResourceName)
				spaceCacheLock.Unlock()
			}
			return nil
		},
		delete: func(key string) (err error) {
			spaceSplit := strings.Split(key, "/")
			dbIDStr := spaceSplit[len(spaceSplit)-2]
			dbID := cast.ToInt64(dbIDStr)
			spaceIDStr := spaceSplit[len(spaceSplit)-1]
			spaceID := cast.ToInt64(spaceIDStr)
			for k, v := range cliCache.spaceCache.Items() {
				if v.Object.(*entity.Space).DBId == dbID && v.Object.(*entity.Space).Id == spaceID {
					log.Info("remove space cache dbID:[%d] space:[%d] ", dbID, spaceID)
					spaceCacheLock.Lock()
					cliCache.spaceCache.Delete(k)
					cliCache.spaceIDCache.Delete(cast.ToString(spaceID))
					spaceCacheLock.Unlock()
					break
				}
			}
			return nil
		},
	}
	spaceJob.start()

	//init partition
	if err := cliCache.initPartition(ctx); err != nil {
		return err
	}
	partitionJob := watcherJob{ctx: ctx, prefix: entity.PrefixPartition, masterClient: cliCache.mc, cache: cliCache.partitionCache,
		put: func(value []byte) (err error) {
			partition := &entity.Partition{}
			if err = cbjson.Unmarshal(value, partition); err != nil {
				return
			}
			space, err := cliCache.mc.QuerySpaceByID(ctx, partition.DBId, partition.SpaceId)
			if err != nil {
				return
			}
			cacheKey := cachePartitionKey(space.Name, partition.Id)
			if old, b := cliCache.partitionCache.Get(cacheKey); !b || partition.UpdateTime > old.(*entity.Partition).UpdateTime {
				cliCache.partitionCache.Set(cacheKey, partition, cache.NoExpiration)
			}
			return nil
		},
		delete: func(key string) (err error) {
			partitionIdSplit := strings.Split(key, "/")
			partitionIdStr := partitionIdSplit[len(partitionIdSplit)-1]
			for k := range cliCache.partitionCache.Items() {
				if strings.HasSuffix(k, "/"+partitionIdStr) {
					cliCache.partitionCache.Delete(k)
					break
				}
			}
			return nil
		},
	}
	partitionJob.start()

	//init server
	if err := cliCache.initServer(ctx); err != nil {
		return err
	}
	serverJob := watcherJob{ctx: ctx, prefix: entity.PrefixServer, masterClient: cliCache.mc, cache: cliCache.serverCache,
		put: func(value []byte) (err error) {
			defer errutil.CatchError(&err)
			server := &entity.Server{}
			if err := cbjson.Unmarshal(value, server); err != nil {
				return err
			}
			if server.ResourceName != config.Conf().Global.ResourceName {
				log.Info("server ip [%v] resource name don't match [%s], [%s] ",
					server.Ip, server.ResourceName, config.Conf().Global.ResourceName)
				return nil
			}
			if value, ok := cliCache.Load(server.ID); ok {
				if value != nil && value.(*rpcClient).client.GetAddress(0) != server.RpcAddr() {
					value.(*rpcClient).close()
					cliCache.Delete(server.ID)
				}
			}
			cliCache.serverCache.Set(cacheServerKey(server.ID), server, cache.NoExpiration)
			return nil
		},
		delete: func(key string) (err error) {
			defer errutil.CatchError(&err)
			serverSplit := strings.Split(key, "/")
			nodeIdStr := serverSplit[len(serverSplit)-1]
			nodeId := cast.ToUint64(nodeIdStr)
			if value, _ := cliCache.Load(nodeId); value != nil {
				value.(*rpcClient).close()
				cliCache.Delete(nodeId)
			}
			cliCache.serverCache.Delete(nodeIdStr)
			return nil
		},
	}
	serverJob.start()

	log.Info("cache inited ok use time %v", time.Since(start))

	return nil
}

func (cliCache *clientCache) stopCacheJob() {
	log.Info("to stop cache job......")
	spaceCacheLock.Lock()
	defer spaceCacheLock.Unlock()
	cliCache.cancel()
}

func (cliCache *clientCache) initUser(ctx context.Context) error {
	_, users, err := cliCache.mc.PrefixScan(ctx, entity.PrefixUser)
	if err != nil {
		log.Error("init user cache err: %s", err.Error())
		return err
	}
	for _, v := range users {
		user := &entity.User{}
		err := cbjson.Unmarshal(v, user)
		if err != nil {
			log.Error("init user cache err: %s", err.Error())
			return err
		}
		if err := cliCache.userCache.Add(user.Name, user, cache.NoExpiration); err != nil {
			log.Error(err.Error())
			return err
		}
	}

	return nil
}

func (cliCache *clientCache) initSpace(ctx context.Context) error {
	spaces, err := cliCache.mc.QuerySpacesByKey(ctx, entity.PrefixSpace)
	if err != nil {
		return err
	}
	for _, s := range spaces {
		db, err := cliCache.mc.QueryDBId2Name(ctx, s.DBId)
		if err != nil {
			log.Error("init spaces cache dbid to id err , err:[%s]", err.Error())
			continue
		}

		if s.ResourceName != config.Conf().Global.ResourceName {
			log.Debug("space name [%s] resource name don't match [%s], [%s], space init ignore. ",
				s.Name, s.ResourceName, config.Conf().Global.ResourceName)
			continue
		}

		spaceCacheLock.Lock()
		if err := cliCache.spaceCache.Add(cacheSpaceKey(db, s.Name), s, cache.NoExpiration); err != nil {
			log.Error(err.Error())
		} else {
			cliCache.spaceIDCache.Set(cast.ToString(s.Id), s, cache.NoExpiration)
		}
		spaceCacheLock.Unlock()
	}
	return nil
}

func (cliCache *clientCache) initPartition(ctx context.Context) error {
	_, values, err := cliCache.mc.PrefixScan(ctx, entity.PrefixPartition)
	if err != nil {
		log.Error("init partition cache err , err:[%s]", err.Error())
		return err
	}
	spaceNameMap := make(map[entity.SpaceID]string)

	for _, bs := range values {
		pt := &entity.Partition{}
		err := cbjson.Unmarshal(bs, pt)
		if err != nil {
			log.Error("init partition cache err , err:[%s]", err.Error())
			continue
		}
		spaceName := spaceNameMap[pt.SpaceId]
		if spaceName == "" {
			space, err := cliCache.mc.QuerySpaceByID(ctx, pt.DBId, pt.SpaceId)
			if err != nil {
				log.Error("partition can not find space by DBID:[%d] spaceID:[%d] partitionID:[%d] err:[%s]", pt.DBId, pt.SpaceId, pt.Id, err.Error())
				continue
			}
			spaceName, spaceNameMap[pt.SpaceId] = space.Name, space.Name
		}
		key := cachePartitionKey(spaceName, pt.Id)
		if err := cliCache.partitionCache.Add(key, pt, cache.NoExpiration); err != nil {
			log.Error(err.Error())
		}
	}

	return nil
}

func (cliCache *clientCache) initServer(ctx context.Context) error {
	_, values, err := cliCache.mc.PrefixScan(ctx, entity.PrefixServer)
	if err != nil {
		log.Error("init server cache err , err:[%s]", err.Error())
		return err
	}
	for _, bs := range values {
		server := &entity.Server{}
		err := cbjson.Unmarshal(bs, server)
		if err != nil {
			log.Error("unmarshal server cache err [%s]", err.Error())
			continue
		}
		if err := cliCache.serverCache.Add(cast.ToString(server.ID), server, cache.NoExpiration); err != nil {
			log.Error(err.Error())
		}
	}
	return nil
}

func (cliCache *clientCache) DeleteSpaceCache(ctx context.Context, db, space string) {
	spaceCacheLock.Lock()
	cliCache.spaceCache.Delete(cacheSpaceKey(db, space))
	spaceCacheLock.Unlock()
}

type watcherJob struct {
	ctx          context.Context
	prefix       string
	masterClient *masterClient
	wg           sync.WaitGroup
	cache        *cache.Cache
	put          func(value []byte) (err error)
	delete       func(key string) (err error)
}

// watch /server/ put
func (w *watcherJob) serverPut(value []byte) (e error) {
	// process panic
	defer errutil.CatchError(&e)
	// parse server info
	server := &entity.Server{}
	err := cbjson.Unmarshal(value, server)
	if err != nil {
		panic(err)
	}
	// mutex ensure only one master update the meta, the other just undate local cache
	mutex := w.masterClient.Client().Master().NewLock(w.ctx, entity.ClusterWatchServerKey, time.Second*188)
	if getLock, err := mutex.TryLock(); getLock && err == nil {
		defer func() {
			if err := mutex.Unlock(); err != nil {
				log.Error("failed to unlock space,the Error is:%v ", err)
			}
		}()
		log.Debug("get LOCK success, process fail server %+v ", server)

		// get failServer info
		if len(server.PartitionIds) == 0 {
			// recover failServer
			// if the partition num of the newNode is empty, then recover data by it.
			if config.Conf().Global.AutoRecoverPs {
				err = w.masterClient.RecoverByNewServer(w.ctx, server)
				if err != nil {
					log.Debug("auto recover is err %v,server is %+v", err, server)
				} else {
					log.Info("recover is success, server is %+v", server)
				}
			}
		} else {
			// if failserver recover, then remove record
			w.masterClient.TryRemoveFailServer(w.ctx, server)
		}
	} else {
		log.Debug("get LOCK error, just update cache %+v ", server)
	}
	// update the cache
	w.cache.Set(cacheServerKey(server.ID), server, cache.NoExpiration)
	return err
}

// watch /server/ delete
func (w *watcherJob) serverDelete(cacheKey string) (err error) {
	// process panic
	defer errutil.CatchError(&err)
	nodeID := cast.ToUint64(strings.Split(cacheKey, "/")[2])
	// mutex ensure only one master update the meta, the other just update local cache
	mutex := w.masterClient.Client().Master().NewLock(w.ctx, entity.ClusterWatchServerKey, time.Second*188)
	if getLock, err := mutex.TryLock(); getLock && err == nil {
		defer func() {
			if err := mutex.Unlock(); err != nil {
				log.Error("failed to unlock space,the Error is:%v ", err)
			}
		}()
		log.Debug("get LOCK success, record fail server %d ", nodeID)
		get, found := w.cache.Get(cacheServerKey(nodeID))
		if !found || get == nil {
			return nil
		}
		server := get.(*entity.Server)
		// attach alive, timeout is 5s
		if IsLive(server.RpcAddr()) || len(server.PartitionIds) == 0 {
			log.Info("%+v is alive or server partition is zero.", server)
			return nil
		}
		failServer := &entity.FailServer{ID: server.ID, TimeStamp: time.Now().Unix()}
		failServer.Node = server
		value, err := sonic.Marshal(failServer)
		errutil.ThrowError(err)
		key := entity.FailServerKey(server.ID)
		// put fail node info into etcd
		err = w.masterClient.Put(w.ctx, key, value)
		errutil.ThrowError(err)
		log.Info("put failServer %s is success, value is %s", key, string(value))
	} else {
		log.Debug("get LOCK failed, just update cache %d", nodeID)
	}
	log.Info("remove node meta is success, nodeId is %d", nodeID)
	// update the cache
	w.cache.Delete(cacheServerKey(nodeID))
	return err
}

func (wj *watcherJob) start() {
	go func() {
		defer func() {
			if rErr := recover(); rErr != nil {
				log.Error("recover() err:[%v]", rErr)
				log.Error("stack:[%s]", debug.Stack())
			}
		}()
		for {
			select {
			case <-wj.ctx.Done():
				log.Debug("watchjob job to stop")
				return
			default:
				log.Debug("start watcher routine")
			}

			wj.wg.Add(1)
			go func() {
				defer func() {
					if rErr := recover(); rErr != nil {
						log.Error("recover() err:[%v]", rErr)
						log.Error("stack:[%s]", debug.Stack())
					}
				}()
				defer wj.wg.Done()

				select {
				case <-wj.ctx.Done():
					log.Debug("watchjob job to stop")
					return
				default:
				}

				watcher, err := wj.masterClient.WatchPrefix(wj.ctx, wj.prefix)

				if err != nil {
					log.Error("watch prefix:[%s] err", wj.prefix)
					time.Sleep(1 * time.Second)
					return
				}

				for reps := range watcher {
					if reps.Canceled {
						log.Error("chan is closed by server watcher job")
						return
					}

					for _, event := range reps.Events {
						switch event.Type {
						case mvccpb.PUT:
							err := wj.put(event.Kv.Value)
							if err != nil {
								log.Error("change cache %s, err: %s , content: %s", wj.prefix, err.Error(), string(event.Kv.Value))
							}

						case mvccpb.DELETE:
							err := wj.delete(string(event.Kv.Key))
							if err != nil {
								log.Error("delete cache %s, err: %s , content: %s", wj.prefix, err.Error(), string(event.Kv.Value))
							}
						}
					}
				}
			}()
			wj.wg.Wait()
		}
	}()

}

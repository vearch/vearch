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
	"github.com/patrickmn/go-cache"
	"github.com/vearch/vearch/proto"
	"github.com/vearch/vearch/util/cbjson"
	"github.com/vearch/vearch/util/vearchlog"
	"strconv"
	"strings"
	"sync"

	"github.com/spf13/cast"
	"github.com/vearch/vearch/util/log"
	. "github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/util/atomic"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"runtime/debug"
	"time"
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

type spaceEntry struct {
	lastUpdateTime time.Time
	mutex          sync.Mutex
	refCount       *atomic.AtomicInt64
}

func cachePartitionKey(space string, pid PartitionID) string {
	return space + "/" + strconv.FormatInt(int64(pid), 10)
}

func cacheSpaceKey(db, space string) string {
	return db + "/" + space
}

func cacheServerKey(nodeId NodeID) string {
	return cast.ToString(nodeId)
}

//find a user by cache
func (cliCache *clientCache) UserByCache(ctx context.Context, userName string) (*User, error) {

	get, found := cliCache.userCache.Get(userName)
	if found {
		return get.(*User), nil
	}

	_ = cliCache.reloadUserCache(ctx, false, userName)

	for i := 0; i < retryNum; i++ {
		time.Sleep(retrySleepTime)
		log.Debug("to find user by key:[%s] ", userName)
		if get, found = cliCache.spaceCache.Get(userName); found {
			return get.(*User), nil
		}
	}

	return nil, fmt.Errorf("user:[%s] err:[%s]", userName, pkg.CodeErr(pkg.ERRCODE_PARTITION_NOT_EXIST))
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
	} else {
		_, ok := userReloadWorkder.LoadOrStore(userName, struct{}{})
		if ok {
			return nil
		}

		go func() {
			defer userReloadWorkder.Delete(userName)
			vearchlog.FunIfNotNil(fun)
		}()
	}

	return nil
}

//find a space by db and space name , if not exist so query it from db
func (cliCache *clientCache) SpaceByCache(ctx context.Context, db, space string) (*Space, error) {
	key := cacheSpaceKey(db, space)

	get, found := cliCache.spaceCache.Get(key)
	if found {
		return get.(*Space), nil
	}

	vearchlog.LogErrNotNil(cliCache.reloadSpaceCache(ctx, false, db, space))

	for i := 0; i < retryNum; i++ {
		time.Sleep(retrySleepTime)
		log.Debug("to find space by key:[%s] ", key)
		if get, found = cliCache.spaceCache.Get(key); found {
			return get.(*Space), nil
		}
	}

	return nil, fmt.Errorf("db:[%s] space:[%s] err:[%s]", db, space, pkg.CodeErr(pkg.ERRCODE_SPACE_NOTEXISTS))
}

func (cliCache *clientCache) reloadSpaceCache(ctx context.Context, sync bool, db string, space string) error {
	key := cacheSpaceKey(db, space)

	fun := func() error {

		log.Info("to reload db:[%s] space:[%s]", db, space)

		dbID, err := cliCache.mc.QueryDBName2Id(ctx, db)
		if err != nil {
			return fmt.Errorf("can not found db by name:[%s] err:[%s]", db, err.Error())
		}

		space, err := cliCache.mc.QuerySpaceByName(ctx, dbID, space)
		if err != nil {
			return fmt.Errorf("can not found db by name:[%s] err:[%s]", db, err.Error())
		}
		spaceCacheLock.Lock()
		defer spaceCacheLock.Unlock()
		cliCache.spaceCache.Set(key, space, cache.NoExpiration)
		cliCache.spaceIDCache.Set(cast.ToString(space.Id), space, cache.NoExpiration)
		return nil
	}

	if sync {
		return fun()
	} else {
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
	}

	return nil
}

//partition/[spaceId]/[id]:[body]
func (cliCache *clientCache) PartitionByCache(ctx context.Context, spaceName string, pid PartitionID) (*Partition, error) {
	key := cachePartitionKey(spaceName, pid)
	get, found := cliCache.partitionCache.Get(key)
	if found {
		return get.(*Partition), nil
	}

	_ = cliCache.reloadPartitionCache(ctx, false, spaceName, pid)

	for i := 0; i < retryNum; i++ {
		time.Sleep(retrySleepTime)
		log.Debug("to find partition by key:[%s] ", key)
		if get, found = cliCache.partitionCache.Get(key); found {
			return get.(*Partition), nil
		}
	}

	return nil, fmt.Errorf("space:[%s] partition_id:[%d] err:[%s]", spaceName, pid, pkg.CodeErr(pkg.ERRCODE_PARTITION_NOT_EXIST))
}

func (cliCache *clientCache) reloadPartitionCache(ctx context.Context, sync bool, spaceName string, pid PartitionID) error {
	key := cachePartitionKey(spaceName, pid)

	fun := func() error {

		log.Info("to reload space:[%s] partition_id:[%d] ", spaceName, pid)

		ctx, _ = context.WithTimeout(ctx, 10*time.Second)

		partition, err := cliCache.mc.QueryPartition(ctx, pid)
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

func (cliCache *clientCache) ServerByCache(ctx context.Context, id NodeID) (*Server, error) {
	key := cast.ToString(id)
	get, found := cliCache.serverCache.Get(key)
	if found {
		return get.(*Server), nil
	}

	_ = cliCache.reloadServerCache(ctx, false, id)

	for i := 0; i < retryNum; i++ {
		time.Sleep(retrySleepTime)
		log.Debug("to find server by key:[%s] ", key)
		if get, found = cliCache.serverCache.Get(key); found {
			return get.(*Server), nil
		}
	}

	return nil, fmt.Errorf("node_id:[%d] err:[%s]", id, pkg.CodeErr(pkg.ERRCODE_PARTITION_NOT_EXIST))
}

func (cliCache *clientCache) reloadServerCache(ctx context.Context, sync bool, id NodeID) error {
	key := cast.ToString(id)

	fun := func() error {

		log.Info("to reload server:[%d] ", id)

		ctx, _ = context.WithTimeout(ctx, 10*time.Second)
		server, err := cliCache.mc.QueryServer(ctx, id)
		if err != nil {
			return fmt.Errorf("can not found server node_id:[%d] err:[%s]", id, err.Error())
		}

		cliCache.serverCache.Set(key, server, cache.NoExpiration)

		return nil
	}

	if sync {
		return fun()
	} else {
		if _, ok := serverReloadWorkder.LoadOrStore(key, struct{}{}); ok {
			return nil
		}
		go func() {
			defer serverReloadWorkder.Delete(key)
			vearchlog.FunIfNotNil(fun)
		}()
	}

	return nil
}

//it will start cache
func (cliCache *clientCache) startCacheJob(ctx context.Context) error {
	log.Info("to start cache job begin")
	start := time.Now()

	//init user
	if err := cliCache.initUser(ctx); err != nil {
		return err
	}
	userJob := watcherJob{ctx: ctx, prefix: PrefixUser, masterClient: cliCache.mc, cache: cliCache.userCache,
		put: func(value []byte) (err error) {
			user := &User{}
			if err := cbjson.Unmarshal(value, user); err != nil {
				return fmt.Errorf("put event user cache err, can't unmarshal event value: %s , error: %s", string(value), err.Error())
			}
			cliCache.userCache.Set(UserKey(user.Name), user, cache.NoExpiration)
			return nil
		},
		delete: func(key string) (err error) {
			userSplit := strings.Split(key, "/")
			if len(userSplit) != 3 {
				log.Error("user delete event got err key")
			}
			username := userSplit[2]
			cliCache.userCache.Delete(username)
			return nil
		},
	}

	userJob.start()

	//init space
	if err := cliCache.initSpace(ctx); err != nil {
		return err
	}
	spaceJob := watcherJob{ctx: ctx, prefix: PrefixSpace, masterClient: cliCache.mc, cache: cliCache.spaceCache,
		put: func(value []byte) (err error) {
			space := &Space{}
			if err := cbjson.Unmarshal(value, space); err != nil {
				return err
			}
			dbName, err := cliCache.mc.QueryDBId2Name(ctx, space.DBId)
			if err != nil {
				return fmt.Errorf("change cache space err: %s , not found db content: %s", err.Error(), string(value))
			}
			key := cacheSpaceKey(dbName, space.Name)
			if oldValue, b := cliCache.spaceCache.Get(key); !b || space.Version > oldValue.(*Space).Version {
				spaceCacheLock.Lock()
				cliCache.spaceCache.Set(key, space, cache.NoExpiration)
				cliCache.spaceIDCache.Set(cast.ToString(space.Id), space, cache.NoExpiration)
				spaceCacheLock.Unlock()
			}
			return nil
		},
		delete: func(key string) (err error) {
			dbIdStr := strings.Split(key, "/")[2]
			dbId := cast.ToInt64(dbIdStr)
			spaceIdStr := strings.Split(key, "/")[3]
			spaceId := cast.ToInt64(spaceIdStr)
			for k, v := range cliCache.spaceCache.Items() {
				if v.Object.(*Space).DBId == dbId && v.Object.(*Space).Id == spaceId {
					log.Info("remove space cache dbID:[%d] space:[%d] ", dbId, spaceId)
					spaceCacheLock.Lock()
					cliCache.spaceCache.Delete(k)
					cliCache.spaceIDCache.Delete(cast.ToString(spaceId))
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
	partitionJob := watcherJob{ctx: ctx, prefix: PrefixPartition, masterClient: cliCache.mc, cache: cliCache.partitionCache,
		put: func(value []byte) (err error) {
			partition := &Partition{}
			if err = cbjson.Unmarshal(value, partition); err != nil {
				return
			}
			space, err := cliCache.mc.QuerySpaceById(ctx, partition.DBId, partition.SpaceId)
			if err != nil {
				return
			}
			cacheKey := cachePartitionKey(space.Name, partition.Id)
			if old, b := cliCache.partitionCache.Get(cacheKey); !b || partition.UpdateTime > old.(*Partition).UpdateTime {
				cliCache.partitionCache.Set(cacheKey, partition, cache.NoExpiration)
			}
			return nil
		},
		delete: func(key string) (err error) {
			partitionIdStr := strings.Split(key, "/")[2]
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
	serverJob := watcherJob{ctx: ctx, prefix: PrefixServer, masterClient: cliCache.mc, cache: cliCache.serverCache,
		put: func(value []byte) (err error) {
			server := &Server{}
			if err := cbjson.Unmarshal(value, server); err != nil {
				return err
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
		delete: func(cacheKey string) (err error) {
			nodeIdStr := strings.Split(cacheKey, "/")[2]
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

	log.Info("cache inited ok use time %v", time.Now().Sub(start))

	return nil
}

func (cliCache *clientCache) stopCacheJob() {
	log.Info("to stop cache job......")
	spaceCacheLock.Lock()
	defer spaceCacheLock.Unlock()
	cliCache.cancel()
}

func (cliCache *clientCache) initUser(ctx context.Context) error {
	_, users, err := cliCache.mc.PrefixScan(ctx, PrefixUser)
	if err != nil {
		log.Error("init user cache err: %s", err.Error())
		return err
	}
	for _, v := range users {
		user := &User{}
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
	spaces, err := cliCache.mc.QuerySpacesByKey(ctx, PrefixSpace)
	if err != nil {
		return err
	}
	for _, s := range spaces {
		db, err := cliCache.mc.QueryDBId2Name(ctx, s.DBId)
		if err != nil {
			log.Error("init spaces cache dbid to id err , err:[%s]", err.Error())
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
	_, values, err := cliCache.mc.PrefixScan(ctx, PrefixPartition)
	if err != nil {
		log.Error("init partition cache err , err:[%s]", err.Error())
		return err
	}
	spaceNameMap := make(map[SpaceID]string)

	for _, bs := range values {
		pt := &Partition{}
		err := cbjson.Unmarshal(bs, pt)
		if err != nil {
			log.Error("init partition cache err , err:[%s]", err.Error())
			continue
		}
		spaceName := spaceNameMap[pt.SpaceId]
		if spaceName == "" {
			space, err := cliCache.mc.QuerySpaceById(ctx, pt.DBId, pt.SpaceId)
			if err != nil {
				log.Error("partition can not find space by DBID:[%d] spaceID:[%pt.SpaceId] partitionID:[%d] err:[%s]", pt.DBId, pt.SpaceId, pt.Id, err.Error())
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
	_, values, err := cliCache.mc.PrefixScan(ctx, PrefixServer)
	if err != nil {
		log.Error("init server cache err , err:[%s]", err.Error())
		return err
	}
	for _, bs := range values {
		server := &Server{}
		err := cbjson.Unmarshal(bs, server)
		if err != nil {
			log.Error("unmarshal server cache err , err:[%s]", err.Error())
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

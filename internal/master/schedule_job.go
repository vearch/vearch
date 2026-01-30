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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/patrickmn/go-cache"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/v3/internal/client"
	"github.com/vearch/vearch/v3/internal/config"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/monitor"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/pkg/vjson"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/client/v3/concurrency"
)

const CronInterval = 60

func walkPartitions(masterServer *Server, partitions []*entity.Partition) {
	ctx := masterServer.ctx
	log.Debug("Start Walking Partitions!")
	for _, partition := range partitions {
		if space, err := masterServer.client.Master().QuerySpaceByID(ctx, partition.DBId, partition.SpaceId); err != nil {
			if vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, err).GetError().Code == vearchpb.ErrorEnum_SPACE_NOT_EXIST {
				log.Warnf("Could not find Space contains partition,PartitionID:[%d] so remove it from etcd!", partition.Id)
				partitionKey := entity.PartitionKey(partition.Id)
				if err := masterServer.client.Master().Delete(ctx, partitionKey); err != nil {
					log.Warnf("error:%s", err.Error())
				}
			} else {
				log.Warnf("Failed to find space according dbid:[%d] spaceid:[%d] partitionID:[%d] err:[%s]", partition.DBId, partition.SpaceId, partition.Id, err.Error())
			}
		} else {
			if space == nil {
				log.Warnf("Could not find Space contains partition,PartitionID:[%d] so remove it from etcd!", partition.Id)
				partitionKey := entity.PartitionKey(partition.Id)
				if err := masterServer.client.Master().Delete(ctx, partitionKey); err != nil {
					log.Warnf(err.Error())
				}
			}
		}
	}
	log.Debug("Complete Walking Partitions!")
}

func walkSpaces(masterServer *Server, spaces []*entity.Space) {
	ctx := masterServer.ctx
	log.Debug("Start Walking Spaces!")
	for _, space := range spaces {
		if db, err := masterServer.client.Master().Get(ctx, entity.DBKeyBody(space.DBId)); err != nil {
			log.Warnf("Failed to get key[%s] from etcd, err: [%s]", entity.DBKeyBody(space.DBId), err.Error())
		} else if db == nil {
			log.Warnf("Could not find database contains space, SpaceName: %s, SpaceID: %s, so remove it!", space.Name, space.Id)
			spaceKey := entity.SpaceKey(space.DBId, space.Id)
			if err := masterServer.client.Master().Delete(ctx, spaceKey); err != nil {
				log.Warnf("error: %s", err.Error())
			}
		}
	}
	log.Debug("Complete Walking Spaces!")
}

func removePartition(partitionServerRpcAddr string, pid entity.PartitionID) error {
	log.Debugf("Removing partition:[%s] from ps:[%s]", pid, partitionServerRpcAddr)
	return client.DeletePartition(partitionServerRpcAddr, pid)
}

func walkServers(masterServer *Server, servers []*entity.Server) {
	ctx := masterServer.ctx
	log.Debug("Start Walking Servers!")
	for _, server := range servers {
		for _, pid := range server.PartitionIds {
			if _, err := masterServer.client.Master().QueryPartition(ctx, pid); err != nil {
				if vearchpb.NewError(vearchpb.ErrorEnum_INTERNAL_ERROR, err).GetError().Code == vearchpb.ErrorEnum_PARTITION_NOT_EXIST {
					log.Warnf("to remove partition:%d", pid)
					if err := removePartition(server.RpcAddr(), pid); err != nil {
						log.Warnf("Failed to remove partition: %v allocated on server: %v, and err is:%v", pid, server.ID, err)
					}
				} else {
					log.Warnf("Failed to find partition: %v, allocated on server: %v, err: %v", pid, server.ID, err)
				}
			}
		}
	}
	log.Debug("Complete Walking Servers!")
}

var errSkipJob = errors.New("skip job")

func CleanTask(masterServer *Server) {
	var err = masterServer.client.Master().STM(masterServer.ctx, func(stm concurrency.STM) error {
		timeBytes := stm.Get(entity.ClusterCleanJobKey)
		if len(timeBytes) == 0 {
			return nil
		}

		value := binary.LittleEndian.Uint16([]byte(timeBytes))

		if time.Now().UnixNano() > int64(value) {
			bytes := make([]byte, 8)
			binary.LittleEndian.PutUint64(bytes, uint64(time.Now().UnixNano()+int64(CronInterval)))
			stm.Put(entity.ClusterCleanJobKey, string(bytes))
			return nil
		}
		return errSkipJob
	})
	if err == errSkipJob {
		log.Debug("skip clean task .....")
		return
	}
	if err != nil {
		log.Errorf("clean task has err for get ClusterCleanJobKey err: %s", err.Error())
		return
	}

	log.Debug("Start clean task")
	//process partitions
	if partitions, err := masterServer.client.Master().QueryPartitions(masterServer.ctx); err != nil {
		log.Errorf("Failed to get all partitions,err: %s", err.Error())
	} else {
		walkPartitions(masterServer, partitions)
	}

	//process spaces
	if spaces, err := masterServer.client.Master().QuerySpacesByKey(masterServer.ctx, entity.PrefixSpace); err != nil {
		log.Errorf("Failed to get all spaces,err: %s", err.Error())
	} else {
		walkSpaces(masterServer, spaces)
	}

	//process servers
	if servers, err := masterServer.client.Master().QueryServers(masterServer.ctx); err != nil {
		log.Errorf("Failed to get all servers,err: %s", err.Error())
	} else {
		walkServers(masterServer, servers)
	}
}

// WatchServerJob watch ps server put and delete
func (s *Server) WatchServerJob(ctx context.Context, cli *client.Client) error {
	err := client.NewWatchServerCache(ctx, cli)
	if err != nil {
		return err
	}
	return nil
}

type clientCache struct {
	sync.Map
	cli        *client.Client
	cancel     context.CancelFunc
	lock       sync.Mutex
	spaceCache *cache.Cache
}

type watcherJob struct {
	ctx    context.Context
	prefix string
	cli    *client.Client
	wg     sync.WaitGroup
	cache  *cache.Cache
	put    func(key, value []byte) (err error)
	delete func(key string) (err error)
}

func (cliCache *clientCache) initSpace(ctx context.Context) error {
	spaces, err := cliCache.cli.Master().QuerySpacesByKey(ctx, entity.PrefixSpace)
	if err != nil {
		return err
	}
	for _, s := range spaces {
		db, err := cliCache.cli.Master().QueryDBId2Name(ctx, s.DBId)
		if err != nil {
			log.Error("init spaces cache dbid to id err , err:[%s]", err.Error())
			continue
		}

		if s.ResourceName != config.Conf().Global.ResourceName {
			log.Debug("space name [%s] resource name don't match [%s], [%s], space init ignore. ",
				s.Name, s.ResourceName, config.Conf().Global.ResourceName)
			continue
		}

		cliCache.lock.Lock()
		if err := cliCache.spaceCache.Add(client.CacheSpaceKey(db, s.Name), s, cache.NoExpiration); err != nil {
			log.Error(err.Error())
		}
		cliCache.lock.Unlock()
	}
	return nil
}

// WatchSpaceJob watch space put and delete
func (s *Server) WatchSpaceJob(ctx context.Context, cli *client.Client) error {
	_, cancel := context.WithCancel(ctx)

	cc := &clientCache{
		cli:        cli,
		cancel:     cancel,
		spaceCache: cache.New(cache.NoExpiration, cache.NoExpiration),
	}

	// init space
	if err := cc.initSpace(ctx); err != nil {
		return err
	}
	spaceJob := watcherJob{ctx: ctx, prefix: entity.PrefixSpace, cli: cli, cache: cc.spaceCache,
		put: func(key, value []byte) (err error) {
			space := &entity.Space{}
			if err := vjson.Unmarshal(value, space); err != nil {
				return err
			}
			if space.ResourceName != config.Conf().Global.ResourceName {
				log.Debug("space name [%s] resource name don't match [%s], [%s] add cache ignore.",
					space.Name, space.ResourceName, config.Conf().Global.ResourceName)
				return nil
			}
			dbName, err := cli.Master().QueryDBId2Name(ctx, space.DBId)
			if err != nil {
				return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("find db by id err: %s, data: %s", err.Error(), string(value)))
			}
			ckey := client.CacheSpaceKey(dbName, space.Name)
			if oldValue, b := cc.spaceCache.Get(ckey); !b || space.Version > oldValue.(*entity.Space).Version {
				cc.lock.Lock()
				cc.spaceCache.Set(ckey, space, cache.NoExpiration)
				log.Debug("space name [%s] , [%s], [%s] add to cache.",
					space.Name, space.ResourceName, config.Conf().Global.ResourceName)
				cc.lock.Unlock()
			}
			return nil
		},
		delete: func(key string) (err error) {
			spaceSplit := strings.Split(key, "/")
			dbIDStr := spaceSplit[len(spaceSplit)-2]
			dbID := cast.ToInt64(dbIDStr)
			spaceIDStr := spaceSplit[len(spaceSplit)-1]
			spaceID := cast.ToInt64(spaceIDStr)
			dbName, err := cli.Master().QueryDBId2Name(ctx, dbID)
			if err != nil {
				log.Error("find db by id err: %s, dbId: %d", err.Error(), dbID)
				return vearchpb.NewError(vearchpb.ErrorEnum_PARAM_ERROR, fmt.Errorf("find db by id err: %s, , dbId: %d", err.Error(), dbID))
			}
			for k, v := range cc.spaceCache.Items() {
				if v.Object.(*entity.Space).DBId == dbID && v.Object.(*entity.Space).Id == spaceID {
					cc.lock.Lock()
					pidMap := make([]entity.PartitionID, 0)
					for _, partition := range v.Object.(*entity.Space).Partitions {
						pidMap = append(pidMap, partition.Id)
					}
					log.Debug("space delete detected, db name : %s, dbID:[%d], space name : %s, space id : %d,  remove related metrics:%v", dbName, dbID, v.Object.(*entity.Space).Name, spaceID, pidMap)
					monitor.RemoveUselessSpaceAndPartitionMetrics(dbName, v.Object.(*entity.Space).Name, pidMap)
					cc.spaceCache.Delete(k)
					cc.lock.Unlock()

					break
				}
			}
			return nil
		},
	}
	spaceJob.start()
	return nil
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
				log.Debug("watchjob job to stop %s", wj.prefix)
				return
			default:
				log.Debug("start watcher routine %s", wj.prefix)
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
					log.Debug("watchjob job to stop %s", wj.prefix)
					return
				default:
				}

				watcher, err := wj.cli.Master().WatchPrefix(wj.ctx, wj.prefix)

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
							err := wj.put(event.Kv.Key, event.Kv.Value)
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

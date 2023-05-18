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
	"time"
    "sync"

	"github.com/vearch/vearch/config"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/ps/psutil"
	"github.com/vearch/vearch/util/log"
	"github.com/vearch/vearch/util/slice"
	"go.etcd.io/etcd/clientv3"
)
var failIpMap sync.Map
var defaultFailBackInverval int = 600
var getAllServersWaitTime int = 30  // sec

// this job for heartbeat master 1m once
func (s *Server) StartHeartbeatJob() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		server := &entity.Server{
			ID:                s.nodeID,
			Ip:                s.ip,
			ResourceName:      config.Conf().Global.ResourceName,
			RpcPort:           config.Conf().PS.RpcPort,
			RaftHeartbeatPort: config.Conf().PS.RaftHeartbeatPort,
			RaftReplicatePort: config.Conf().PS.RaftReplicatePort,
			PartitionIds:      make([]entity.PartitionID, 0, 10),
			Private:           config.Conf().PS.Private,
			Version: &entity.BuildVersion{
				BuildVersion: config.GetBuildVersion(),
				BuildTime:    config.GetBuildTime(),
				CommitID:     config.GetCommitID(),
			},
		}
		var leaseId clientv3.LeaseID = 0
		var lastPartitionIds []entity.PartitionID

		if s.stopping.Get() {
			return
		}

		server.PartitionIds = psutil.GetAllPartitions(config.Conf().GetDatas())
		ctx := context.Background()
		keepaliveC, err := s.client.Master().KeepAlive(ctx, server)
		if err != nil {
			log.Error("KeepAlive err: ", err.Error())
			return
		}
		lastPartitionIds = server.PartitionIds
        go func() {
            defer func() {
                if r := recover(); r != nil {
                    log.Error("fail over task error, will exits...")
                }
            }()
            if config.Conf().PS.EnableFailOver == false {
                log.Info("fail over function is closed...")
                return
            }

            interval := int(config.Conf().PS.FailOverInterval)
            if (interval == 0) {
                interval = defaultFailBackInverval 
            }
            seq := 0
            serverStas := make(map[uint64]int)
            // need to opt the function to get all ps servers
            psCnt := 0
            for i := 0; i <= getAllServersWaitTime; i++ {
                time.Sleep(1 * time.Second)
                servers, _:= s.client.Master().QueryServers(ctx)
                if len(servers) > psCnt {
                    psCnt = len(servers)
                }
            }
            for {
                time.Sleep(1 * time.Second)
                seq++
                failServers, err := s.client.Master().QueryAllFailOverServer(ctx)
                if err != nil {
                    continue
                }
                servers, err := s.client.Master().QueryServers(ctx)
                if err != nil {
                    continue
                }

                partitions, _ := s.client.Master().QueryPartitions(ctx)
                for _, p := range  partitions {
                    if seq % 10 == 0 {
                        log.Debug("check fail partition pid=%d, nodeid=%d. path=%s", p.Id, p.LeaderID, p.Path)
                    }
                }
                for _, fs := range failServers {
                    rec := false
                    for _, as := range servers {
                        if fs.ID == as.ID {
                            rec = true
                            break
                        }
                    }
                    if rec == false {
                        serverStas[fs.ID]++
                    } else {
                        serverStas[fs.ID] = 0
                    }
                    log.Debug("fail over status check %d, fail_pid=%d, cnt=%d, index=%d, cur=%d, pscnt=%d", rec, fs.ID, serverStas[fs.ID], (int(fs.ID)) % psCnt + 1, s.nodeID, psCnt)
                    if serverStas[fs.ID] > interval && (int(fs.ID)) % psCnt + 1 == int(s.nodeID)  {
                        if _, stat := failIpMap.Load(fs.Node.Ip); stat == true {
                            continue
                        }
                        pids := s.recoverFailPartitions(fs.Node.Ip)
                        for _, pid := range pids {
                              s.registerMaster(s.nodeID, pid) 
                        }
                        log.Info("server nodeid=%d ip=%s fail last more than 30 sec", fs.ID, fs.Node.Ip)
                        serverStas[fs.ID] = 0
                        failIpMap.Store(fs.Node.Ip, fs.ID)
                    }
                }
            }
        }()

		go func() {
            failServers, err := s.client.Master().QueryAllFailOverServer(ctx)
            if err != nil {
                log.Info("fail over function is closed...")
            }
            for _, fs := range failServers {
                if (fs.ID == s.nodeID) {
                    s.client.Master().DeleteFailOverByNodeID(ctx, fs.ID)
                }
            }
			for {
                if config.Conf().PS.EnableFailOver == true && config.Conf().PS.EnableFailOverBack == true {
                    partitions, _ := s.client.Master().QueryPartitions(ctx)
                    for _, localPid := range server.PartitionIds  {
                        for _, p := range partitions {
                            if localPid == p.Id && s.nodeID != p.LeaderID {
                               s.registerMaster(s.nodeID, p.Id) 
                            }
                        }
                    }
                }

				time.Sleep(1 * time.Second)
				s.raftResolver.RangeNodes(s.UpdateResolver)

				if leaseId == 0 {
					log.Info("leaseId == 0, continue...")
					continue
				}

                dirs := config.Conf().GetDatas()
                failIpMap.Range(func(key, value interface{}) bool {
                    ip := key.(string)
                    dirs = append(dirs, config.Conf().GetSourceDataDir()  + "/" + ip)
                    return true
                })

				server.PartitionIds = psutil.GetAllPartitions(dirs)

				if slice.EqualUint32(lastPartitionIds, server.PartitionIds) {
					log.Debug("PartitionIds not change, do nothing!")
					continue
				}

				log.Info("server.PartitionIds has changed, need to put server to topo again!, leaseId: [%d]", leaseId)

				if err := s.client.Master().PutServerWithLeaseID(ctx, server, leaseId); err != nil {
					log.Error("PutServerWithLeaseID[leaseId: %d] err:", leaseId, err.Error())
				}

				lastPartitionIds = server.PartitionIds
			}
		}()

		for {
			select {
			case <-ctx.Done():
				log.Error("keep alive ctx done, this ps will can not be found by master!")
				return
			case ka, ok := <-keepaliveC:
				if !ok {
					log.Warn("keep alive channel closed! this ps will connect to master two seconds later.")
					time.Sleep(2 * time.Second)
					keepaliveC, err = s.client.Master().KeepAlive(ctx, server)
					if err != nil {
						log.Warnf("KeepAlive err: %s", err.Error())
					}
					continue
				}
				leaseId = ka.ID
				// log.Debugf("Receive keepalive, leaseId: %d, ttl:%d", ka.ID, ka.TTL)
			}
		}
	}()
}

func (s *Server) UpdateResolver(key, value interface{}) bool {
	id, err := key.(entity.NodeID)
	log.Debugf("update resolver: id: [%v], err: [%v]", id, err)
	if server, err := s.client.Master().QueryServer(context.Background(), id); err != nil {
		log.Error("partition recovery get server info err: %s", err.Error())
	} else {
		s.raftResolver.UpdateNode(id, server.Replica())
	}
	return true
}

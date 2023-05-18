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
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cast"
	"github.com/vearch/vearch/config"
	"github.com/vearch/vearch/master/store"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/proto/vearchpb"
	"github.com/vearch/vearch/util"
	"github.com/vearch/vearch/util/cbjson"
	"github.com/vearch/vearch/util/errutil"
	"github.com/vearch/vearch/util/log"
	"github.com/vearch/vearch/util/netutil"
	"go.etcd.io/etcd/clientv3"
)

const (
	DefaultPsTimeOut = 5
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

// Client return the masterClient.client not masterClient
func (m *masterClient) Client() *Client {
	return m.client
}

// Cache return the clientCache of client
func (m *masterClient) Cache() *clientCache {
	return m.cliCache
}

// FlushCacheJob reset the client.cliCache
func (m *masterClient) FlushCacheJob(ctx context.Context) error {
	cliCache, err := newClientCache(ctx, m)
	if err != nil {
		return err
	}

	old := m.cliCache
	m.cliCache = cliCache
	if old != nil {
		old.stopCacheJob()
	}

	return nil
}

//Stop stop the cache job
func (m *masterClient) Stop() {
	if m.cliCache != nil {
		m.cliCache.stopCacheJob()
	}
}

// QueryDBId2Name query db name from etcd by key /db/id/{dbid}
func (m *masterClient) QueryDBId2Name(ctx context.Context, id int64) (string, error) {
	bytes, err := m.Get(ctx, entity.DBKeyId(id))
	if err != nil {
		return "", err
	}
	if bytes == nil {
		return "", vearchpb.NewError(vearchpb.ErrorEnum_DB_NOTEXISTS, nil)
	}
	return string(bytes), nil
}

// QueryDBName2Id query db id from etcd by key /db/name/{db name}
func (m *masterClient) QueryDBName2Id(ctx context.Context, name string) (int64, error) {
	if bytes, err := m.Get(ctx, entity.DBKeyName(name)); err != nil {
		return -1, err
	} else if bytes == nil {
		return -1, vearchpb.NewError(vearchpb.ErrorEnum_DB_NOTEXISTS, nil)
	} else {
		return cast.ToInt64E(string(bytes))
	}
}

// QueryPartition query partition from etcd by key /partition/{partitionID}
func (m *masterClient) QueryPartition(ctx context.Context, partitionID entity.PartitionID) (*entity.Partition, error) {
	log.Debug("server t 1")
	bytes, err := m.Get(ctx, entity.PartitionKey(partitionID))
	if err != nil {
      	        log.Debug("server t 2")
	        log.Error("query partition error is: %s", err.Error())
		return nil, err
	}
	if bytes == nil {
	        log.Debug("server t 3")
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_PARTITION_NOT_EXIST, nil)
	}

	log.Debug("server t 4, %s", bytes)
	p := new(entity.Partition)
	err = json.Unmarshal(bytes, p)
	return p, err
}

// QueryServer query server from etcd by key /server/{id}
func (m *masterClient) QueryServer(ctx context.Context, id entity.NodeID) (*entity.Server, error) {
	bytes, err := m.Get(ctx, entity.ServerKey(id))
	if err != nil {
		log.Error("QueryServer() error, can not connect master, nodeId:[%d], err:[%v]", id, err)
		return nil, err
	}
	if bytes == nil {
		log.Error("server can not find on master, maybe server is offline, nodeId:[%d]", id)
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_PS_NOTEXISTS, nil)
	}

	p := new(entity.Server)
	if err = json.Unmarshal(bytes, p); err != nil {
		log.Error("server find on master, but json.Unmarshal(bytes, p) error, nodeId:[%d], bytes:[%s], err:[%v]", id, string(bytes), err)
		return nil, err
	}

	return p, err
}

// QueryUser query user info from etcd by key /user/{username}
func (m *masterClient) QueryUser(ctx context.Context, username string) (*entity.User, error) {
	bytes, err := m.Get(ctx, entity.UserKey(username))
	if bytes == nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_USER_NOT_EXISTS, err)
	}
	user := new(entity.User)
	if err = json.Unmarshal(bytes, user); err != nil {
		return nil, err
	}
	return user, nil
}

// QueryUserByPassword Query user info by /user/{username} and valid password
func (m *masterClient) QueryUserByPassword(ctx context.Context, username, password string) (*entity.User, error) {
	user, err := m.QueryUser(ctx, username)
	if err != nil {
		return nil, err
	}

	if user.Password != password {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_AUTHENTICATION_FAILED, nil)
	}
	return user, nil
}

// QueryServers scan all servers
func (m *masterClient) QueryServers(ctx context.Context) ([]*entity.Server, error) {
	_, bytesServers, err := m.PrefixScan(ctx, entity.PrefixServer)
	if err != nil {
		return nil, err
	}
	servers := make([]*entity.Server, 0, len(bytesServers))
	for _, bs := range bytesServers {
		var s = &entity.Server{}
		if err := json.Unmarshal(bs, s); err != nil {
			log.Error("unmarshl server err: %s", err.Error())
			continue
		}
		servers = append(servers, s)
	}

	return servers, err
}

// QuerySpaces query spaces by dbID
func (m *masterClient) QuerySpaces(ctx context.Context, dbID int64) ([]*entity.Space, error) {
	return m.QuerySpacesByKey(ctx, fmt.Sprintf("%s%d/", entity.PrefixSpace, dbID))
}

// QueryRouter query router ip list by key
func (m *masterClient) QueryRouter(ctx context.Context, key string) ([]string, error) {
	_, bytesRouterIP, err := m.PrefixScan(ctx, fmt.Sprintf("%s%s/", entity.PrefixRouter, key))
	if err != nil {
		return nil, err
	}
	routerIPs := make([]string, 0, len(bytesRouterIP))
	for _, bs := range bytesRouterIP {
		ip := strings.Split(string(bs), ":")[0]
		routerIPs = append(routerIPs, ip)
		log.Debugf("find key: [%s], routerIP: [%s]", key, ip)
	}
	return routerIPs, nil
}

// QuerySpacesByKey scan space by space prefix
func (m *masterClient) QuerySpacesByKey(ctx context.Context, prefix string) ([]*entity.Space, error) {
	_, bytesSpaces, err := m.PrefixScan(ctx, prefix)
	if err != nil {
		return nil, err
	}
	spaces := make([]*entity.Space, 0, len(bytesSpaces))
	for _, bs := range bytesSpaces {
		var space = &entity.Space{}
		if err := json.Unmarshal(bs, space); err != nil {
			log.Error("unmarshl space err: %s", err.Error())
			continue
		}
		spaces = append(spaces, space)
	}
	return spaces, err
}

//delete fail server by nodeID
func (m *masterClient) DeleteFailServerByNodeID(ctx context.Context, nodeID uint64) error {
	return m.Delete(ctx, entity.FailServerKey(nodeID))
}

func (m *masterClient) DeleteFailOverByNodeID(ctx context.Context, nodeID uint64) error {
	return m.Delete(ctx, entity.FailOverServerKey(nodeID))
}

//query fail server by nodeID
func (m *masterClient) QueryFailServerByNodeID(ctx context.Context, nodeID uint64) *entity.FailServer {
	bytesArr, err := m.Get(ctx, entity.FailServerKey(nodeID))
	if err != nil {
		return nil
	}
	fs := &entity.FailServer{}
	if err := json.Unmarshal(bytesArr, fs); err != nil {
		log.Error("unmarshl FailServer err: %s,nodeId is %d", err.Error(), nodeID)
		return nil
	}
	return fs
}

//query server by IPAddr
func (m *masterClient) QueryServerByIPAddr(ctx context.Context, IPAddr string) *entity.FailServer {
	var err error
	defer errutil.CatchError(&err)
	//get all failServer
	failServers, err := m.QueryAllFailServer(ctx)
	for _, fs := range failServers {
		if fs.Node.Ip == IPAddr {
			log.Debug("get fail server info [%+v]", fs)
			return fs
		}
	}

	//get all server
	servers, err := m.QueryServers(ctx)
	for _, server := range servers {
		if server.Ip == IPAddr {
			fs := &entity.FailServer{TimeStamp: time.Now().Unix(), Node: server, ID: server.ID}
			log.Debug("get alive server info [%+v]", fs)
			return fs
		}
	}

	return nil
}

// query all fail server
func (m *masterClient) QueryAllFailServer(ctx context.Context) ([]*entity.FailServer, error) {
	return m.QueryFailServerByKey(ctx, entity.PrefixFailServer)
}

func (m *masterClient) QueryAllFailOverServer(ctx context.Context) ([]*entity.FailServer, error) {
	return m.QueryFailServerByKey(ctx, entity.PrefixFailOverServer)
}

//query fail server by prefix
func (m *masterClient) QueryFailServerByKey(ctx context.Context, prefix string) (fs []*entity.FailServer, e error) {
	// painc process
	defer errutil.CatchError(&e)
	_, bytesArr, err := m.PrefixScan(ctx, prefix)
	errutil.ThrowError(err)
	failServers := make([]*entity.FailServer, 0, len(bytesArr))
	for _, bs := range bytesArr {
		fs := &entity.FailServer{}
		if err := json.Unmarshal(bs, fs); err != nil {
			log.Error("unmarshl FailServer err: %s", err.Error())
			continue
		}
		failServers = append(failServers, fs)
	}

	return failServers, err
}

// put fail server info into etcd
func (m *masterClient) PutFailServerByKey(ctx context.Context, key string, value []byte) error {
	err := m.Put(ctx, key, value)
	return err
}

// QueryDBs scan dbs
func (m *masterClient) QueryDBs(ctx context.Context) ([]*entity.DB, error) {
	_, bytesDBs, err := m.PrefixScan(ctx, entity.PrefixDataBaseBody)
	if err != nil {
		return nil, err
	}
	dbs := make([]*entity.DB, 0, len(bytesDBs))
	for _, bs := range bytesDBs {
		db := &entity.DB{}
		if err := json.Unmarshal(bs, db); err != nil {
			log.Error("decode db err: %s,and the bs is:%s", err.Error(), string(bs))
			continue
		}
		dbs = append(dbs, db)
	}
	return dbs, err
}

//QueryPartitions get all partitions from the etcd
func (m *masterClient) QueryPartitions(ctx context.Context) ([]*entity.Partition, error) {
	_, bytesPartitions, err := m.PrefixScan(ctx, entity.PrefixPartition)
	if err != nil {
		log.Error("prefix scan partition err: %s", err.Error())
		return nil, err
	}
	partitions := make([]*entity.Partition, 0, len(bytesPartitions))
	for _, partitionBytes := range bytesPartitions {
		p := &entity.Partition{}
		if err := json.Unmarshal(partitionBytes, p); err != nil {
			log.Error("unmarshl partition err: %s", err.Error())
			continue
		}
		partitions = append(partitions, p)
	}
	return partitions, err
}

// QuerySpaceByID query space by space id and db id
func (m *masterClient) QuerySpaceByID(ctx context.Context, dbID entity.DBID, spaceID entity.SpaceID) (*entity.Space, error) {
	bytes, err := m.Store.Get(ctx, entity.SpaceKey(dbID, spaceID))
	if bytes == nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_SPACE_NOTEXISTS, err)
	}
	space := &entity.Space{}
	if err := json.Unmarshal(bytes, space); err != nil {
		return nil, err
	}
	return space, nil
}

// QuerySpaceByName query space by space name and db id
func (m *masterClient) QuerySpaceByName(ctx context.Context, dbID int64, spaceName string) (*entity.Space, error) {
	spaces, err := m.QuerySpaces(ctx, dbID)
	if err != nil {
		return nil, err
	}
	for _, s := range spaces {
		if s.Name == spaceName {
			return s, nil
		}
	}
	return nil, vearchpb.NewError(vearchpb.ErrorEnum_SPACE_NOTEXISTS, nil)
}

// KeepAlive attempts to keep the given lease alive forever. If the keepalive responses posted
// to the channel are not consumed promptly the channel may become full. When full, the lease
// client will continue sending keep alive requests to the etcd server, but will drop responses
// until there is capacity on the channel to send more responses.
func (m *masterClient) KeepAlive(ctx context.Context, server *entity.Server) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	bytes, err := json.Marshal(server)
	if err != nil {
		return nil, err
	}
	timeout := m.cfg.PS.PsHeartbeatTimeout
	if timeout <= 0 {
		timeout = DefaultPsTimeOut
	}
	return m.Store.KeepAlive(ctx, entity.ServerKey(server.ID), bytes, time.Second*time.Duration(timeout))
}

// PutServerWithLeaseID PutServerWithLeaseID
func (m *masterClient) PutServerWithLeaseID(ctx context.Context, server *entity.Server, leaseID clientv3.LeaseID) error {
	bytes, err := json.Marshal(server)
	if err != nil {
		return err
	}
	timeout := m.cfg.PS.PsHeartbeatTimeout
	if timeout <= 0 {
		timeout = DefaultPsTimeOut
	}
	return m.Store.PutWithLeaseId(ctx, entity.ServerKey(server.ID), bytes, time.Second*time.Duration(timeout), leaseID)
}

// DBKeys get db url in etcd
func (m *masterClient) DBKeys(id int64, name string) (idKey, nameKey, bodyKey string) {
	idKey = entity.DBKeyId(id)
	nameKey = entity.DBKeyName(name)
	bodyKey = entity.DBKeyBody(id)
	return
}

// Authorization default Authorization
const Authorization = "Authorization"

// Root default root
const Root = "root"

// Register register ps nodeid to master, return servers
func (m *masterClient) Register(ctx context.Context, clusterName string, nodeID entity.NodeID, timeout time.Duration) (server *entity.Server, err error) {
	form := url.Values{}
	form.Add("clusterName", clusterName)
	form.Add("nodeID", cast.ToString(nodeID))

	masterServer.reset()
	var response []byte
	for {

		query := netutil.NewQuery().SetHeader(Authorization, util.AuthEncrypt(Root, m.cfg.Global.Signkey))

		keyNumber, err := masterServer.getKey()
		if err != nil {
			return nil, err
		}
		query.SetAddress(m.cfg.Masters[keyNumber].ApiUrl())

		query.SetMethod(http.MethodPost)
		query.SetQuery(form.Encode())
		query.SetUrlPath("/register")
		query.SetTimeout(60)
		log.Debug("master api Register url: %s", query.GetUrl())
		response, err = query.Do()
		log.Debug("master api Register response: %v", string(response))
		if err == nil {
			log.Debug("master api Register success ")
			break
		}
		log.Warnf("master[%s] api Register err: %v", query.GetUrl(), err)

		masterServer.next()

		time.Sleep(2 * time.Second)
	}

	data, err := parseRegisterData(response)
	if err != nil {
		return nil, err
	}
	server = &entity.Server{}
	if err := cbjson.Unmarshal(data, server); err != nil {
		return nil, err
	}

	return server, nil
}

// RegisterRouter register router nodeid to master, return ip
func (m *masterClient) RegisterRouter(ctx context.Context, clusterName string, timeout time.Duration) (res string, err error) {
	form := url.Values{}
	form.Add("clusterName", clusterName)

	masterServer.reset()
	var response []byte
	for {

		query := netutil.NewQuery().SetHeader(Authorization, util.AuthEncrypt(Root, m.cfg.Global.Signkey))

		keyNumber, err := masterServer.getKey()
		if err != nil {
			return "", err
		}
		query.SetAddress(m.cfg.Masters[keyNumber].ApiUrl())
		query.SetMethod(http.MethodPost)
		query.SetQuery(form.Encode())
		query.SetUrlPath("/register_router")
		query.SetTimeout(60)
		log.Debug("master api Register url: %s", query.GetUrl())
		response, err = query.Do()
		log.Debug("master api Register response: %v", string(response))
		if err == nil {
			break
		}
		log.Debug("master api Register err: %v", err)

		masterServer.next()
	}

	data, err := parseRegisterData(response)
	if err != nil {
		return "", err
	}

	return string(data[1 : len(data)-1]), nil
}

// RegisterPartition register partition
func (m *masterClient) RegisterPartition(ctx context.Context, partition *entity.Partition) error {
	reqBody, err := cbjson.Marshal(partition)
	if err != nil {
		return err
	}

	masterServer.reset()
	var response []byte
	for {
		query := netutil.NewQuery().SetHeader(Authorization, util.AuthEncrypt(Root, m.cfg.Global.Signkey))
		keyNumber, err := masterServer.getKey()
		if err != nil {
			return err
		}
		query.SetAddress(m.cfg.Masters[keyNumber].ApiUrl())
		query.SetMethod(http.MethodPost)
		query.SetUrlPath("/register_partition")
		query.SetReqBody(string(reqBody))
		query.SetContentTypeJson()
		query.SetTimeout(60)
		response, err = query.Do()
		log.Debug("master api register partition response: %v", string(response))
		if err == nil {
			break
		}
		log.Warnf("master api register partition err: %v", err)

		masterServer.next()
	}

	jsonMap, err := cbjson.ByteToJsonMap(response)
	if err != nil {
		return err
	}

	code, err := jsonMap.GetJsonValIntE("code")
	if err != nil {
		return fmt.Errorf("client master api register partiton parse response code error: %s", err.Error())
	}

	if code != vearchpb.ErrCode(vearchpb.ErrorEnum_SUCCESS) {
		return fmt.Errorf("client master api register partiton parse response error, code: %d, msg: %s", code, jsonMap.GetJsonValStringOrDefault("msg", ""))
	}

	return nil
}

//send HTTPPost request
func (m *masterClient) HTTPPost(ctx context.Context, url string, reqBody string) (response []byte, e error) {
	//process panic
	defer func() {
		if info := recover(); info != nil {
			e = fmt.Errorf("panic is %v", info)
		}
	}()
	query := netutil.NewQuery().SetHeader(Authorization, util.AuthEncrypt(Root, m.cfg.Global.Signkey))
	query.SetMethod(http.MethodPost)
	query.SetUrlPath(url)
	query.SetReqBody(reqBody)
	query.SetContentTypeJson()
	query.SetTimeout(60)
	for {
		keyNumber, err := masterServer.getKey()
		if err != nil {
			panic(err)
		}
		query.SetAddress(m.cfg.Masters[keyNumber].ApiUrl())
		log.Debug("remote server url: %s, req body: %s", query.GetUrl(), string(reqBody))
		response, err = query.Do()
		log.Debug("remote server response: %v", string(response))
		if err == nil {
			break
		} else {
			e = err
		}
		log.Debug("remove server err: %v", err)
		masterServer.next()
	}
	return response, e
}

// remove metadata of the node and delete from raftServer
func (m *masterClient) RemoveNodeMeta(ctx context.Context, nodeID entity.NodeID) error {
	if nodeID == 0 {
		return fmt.Errorf("nodeId is zero")
	}
	// begin clear meta about this nodeId
	rfs := &entity.RecoverFailServer{FailNodeID: nodeID}

	reqBody, err := cbjson.Marshal(rfs)
	if err != nil {
		return err
	}
	masterServer.reset()
	response, err := m.HTTPPost(ctx, "/meta/remove_server", string(reqBody))
	log.Debug("remove server response: %v", string(response))
	if err != nil {
		return err
	}
	return nil
}

// failserver recover,remove from etcd record
func (m *masterClient) TryRemoveFailServer(ctx context.Context, server *entity.Server) {
	failServers, err := m.QueryAllFailServer(ctx)
	if err != nil {
		log.Error("QueryAllFailServer err %v ,failserver is %v", server.ID, err)
	}
	for _, fs := range failServers {
		if fs.ID == server.ID && fs.Node.Ip == server.Ip {
			//delete failserver record
			err := m.DeleteFailServerByNodeID(ctx, fs.ID)
			if err != nil {
				log.Error("Delete failserver is %+v, err %v.", fs, err)
			} else {
				log.Debug("Delete failserver is %+v success.", fs)
			}
		}
	}
}

// @description recover a fail server by a new server
// @param server *entity.Server "new server info"
func (client *masterClient) RecoverByNewServer(ctx context.Context, server *entity.Server) (e error) {
	//process panic
	defer errutil.CatchError(&e)
	failServers, err := client.QueryAllFailServer(ctx)
	errutil.ThrowError(err)
	if len(failServers) > 0 {
		fs := failServers[0]
		if fs != nil {
			rfs := &entity.RecoverFailServer{FailNodeAddr: fs.Node.Ip, NewNodeAddr: server.Ip}
			log.Debug("begin recover %s", rfs)
			// if auto recover,need remove node meta data
			err := client.RemoveNodeMeta(ctx, fs.Node.ID)
			errutil.ThrowError(err)
			// recover fail server
			err = client.RecoverFailServer(ctx, rfs)
			errutil.ThrowError(err)
			log.Info("Recover success, nodeID is %s .", fs.ID)
		}
	}
	return nil
}

//@description recover the failserver by newserver
//@param rfs *entity.RecoverFailServer "failserver IPAddr,newserver IPAddr"
func (client *masterClient) RecoverFailServer(ctx context.Context, rfs *entity.RecoverFailServer) (e error) {
	//process panic
	defer errutil.CatchError(&e)
	reqBody, err := cbjson.Marshal(rfs)
	errutil.ThrowError(err)
	masterServer.reset()
	response, err := client.HTTPPost(ctx, "/schedule/recover_server", string(reqBody))
	errutil.ThrowError(err)
	jsonMap, err := cbjson.ByteToJsonMap(response)
	errutil.ThrowError(err)
	_, err = jsonMap.GetJsonValIntE("code")
	errutil.ThrowError(err)
	return e
}

func parseRegisterData(response []byte) ([]byte, error) {
	jsonMap, err := cbjson.ByteToJsonMap(response)
	if err != nil {
		return nil, err
	}

	code, err := jsonMap.GetJsonValIntE("code")
	if err != nil {
		return nil, fmt.Errorf("client master api register parse response code error: %s", err.Error())
	}

	if code != vearchpb.ErrCode(vearchpb.ErrorEnum_SUCCESS) {
		return nil, fmt.Errorf("client master api register parse response error, code: %d, msg: %s", code, jsonMap.GetJsonValStringOrDefault("msg", ""))
	}
	data, err := jsonMap.GetJsonValBytes("data")
	if err != nil {
		return nil, fmt.Errorf("client master api register parse response data error: %s", err.Error())
	}
	return data, nil
}

var masterServer = &MasterServer{}

// MasterServer the num of master
type MasterServer struct {
	total     int
	keyNumber int
	tryTimes  int
}

func (m *MasterServer) init(total int) {
	m.total = total
}

func (m *MasterServer) reset() {
	m.tryTimes = 0
}

func (m *MasterServer) getKey() (int, error) {
	if m.tryTimes >= m.total {
		return 0, fmt.Errorf("master server all down")
	}

	return m.keyNumber, nil
}

func (m *MasterServer) next() {
	m.keyNumber++
	if (m.keyNumber + 1) > m.total {
		m.keyNumber = 0
	}

	m.tryTimes++
}

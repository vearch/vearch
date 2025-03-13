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

package services

import (
	"context"
	"fmt"
	"time"

	"github.com/cubefs/cubefs/depends/tiglabs/raft/proto"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/v3/internal/client"
	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/pkg/errutil"
	"github.com/vearch/vearch/v3/internal/pkg/log"
	"github.com/vearch/vearch/v3/internal/pkg/number"
	json "github.com/vearch/vearch/v3/internal/pkg/vjson"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type DBService struct {
	client *client.Client
}

func NewDBService(client *client.Client) *DBService {
	return &DBService{client: client}
}

func (s *DBService) CreateDB(ctx context.Context, db *entity.DB) (err error) {
	mc := s.client.Master()
	if mc.Client().Master().Config().Global.LimitedDBNum {
		_, bytesArr, err := mc.PrefixScan(ctx, entity.PrefixDataBaseBody)
		if err != nil {
			return err
		}
		if len(bytesArr) >= 1 {
			return fmt.Errorf("db num is limited to one and already have one db exists")
		}
	}

	// validate name has in db is in return err
	if err = db.Validate(); err != nil {
		return err
	}

	if err = s.validatePS(ctx, db.Ps); err != nil {
		return err
	}

	// generate a new db id
	db.Id, err = mc.NewIDGenerate(ctx, entity.DBIdSequence, 1, 5*time.Second)
	if err != nil {
		return err
	}

	// it will lock cluster to create db
	mutex := mc.NewLock(ctx, entity.LockDBKey(db.Name), time.Second*300)
	if err = mutex.Lock(); err != nil {
		return err
	}
	defer func() {
		if err := mutex.Unlock(); err != nil {
			log.Error("unlock db err:[%s]", err.Error())
		}
	}()
	err = mc.STM(context.Background(), func(stm concurrency.STM) error {
		idKey, nameKey, bodyKey := mc.DBKeys(db.Id, db.Name)

		if stm.Get(nameKey) != "" {
			return fmt.Errorf("dbname %s is exists", db.Name)
		}

		if stm.Get(idKey) != "" {
			return fmt.Errorf("dbID %d is exists", db.Id)
		}
		value, err := json.Marshal(db)
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

func (s *DBService) DeleteDB(ctx context.Context, dbName string) error {
	db, err := s.QueryDB(ctx, dbName)
	if err != nil {
		return err
	}

	mc := s.client.Master()
	// it will lock cluster to delete db
	mutex := mc.NewLock(ctx, entity.LockDBKey(dbName), time.Second*300)
	if err = mutex.Lock(); err != nil {
		return err
	}
	defer func() {
		if err := mutex.Unlock(); err != nil {
			log.Error("unlock db err:[%s]", err.Error())
		}
	}()

	spaces, err := mc.QuerySpaces(ctx, db.Id)
	if err != nil {
		return err
	}

	if len(spaces) > 0 {
		return vearchpb.NewError(vearchpb.ErrorEnum_DB_NOT_EMPTY, nil)
	}

	err = mc.STM(context.Background(),
		func(stm concurrency.STM) error {
			idKey, nameKey, bodyKey := mc.DBKeys(db.Id, db.Name)
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

func (s *DBService) QueryDB(ctx context.Context, dbName string) (db *entity.DB, err error) {
	var id int64
	mc := s.client.Master()
	db = &entity.DB{}
	if number.IsNum(dbName) {
		id = cast.ToInt64(dbName)
	} else if id, err = mc.QueryDBName2ID(ctx, dbName); err != nil {
		return nil, err
	}

	bs, err := mc.Get(ctx, entity.DBKeyBody(id))

	if err != nil {
		return nil, err
	}

	if bs == nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_DB_NOT_EXIST, nil)
	}

	if err := json.Unmarshal(bs, db); err != nil {
		return nil, err
	} else {
		return db, nil
	}
}

func (s *DBService) QueryDBs(ctx context.Context) ([]*entity.DB, error) {
	mc := s.client.Master()
	_, bytesArr, err := mc.PrefixScan(ctx, entity.PrefixDataBaseBody)
	if err != nil {
		return nil, err
	}
	dbs := make([]*entity.DB, 0, len(bytesArr))
	for _, bs := range bytesArr {
		db := &entity.DB{}
		if err := json.Unmarshal(bs, db); err != nil {
			log.Error("decode db err: %s,and the bs is:%s", err.Error(), string(bs))
			continue
		}
		dbs = append(dbs, db)
	}

	return dbs, err
}

func (s *DBService) UpdateDBIpList(ctx context.Context, dbModify *entity.DBModify) (db *entity.DB, err error) {
	// process panic
	defer errutil.CatchError(&err)
	var id int64
	db = &entity.DB{}
	mc := s.client.Master()
	if number.IsNum(dbModify.DbName) {
		id = cast.ToInt64(dbModify.DbName)
	} else if id, err = mc.QueryDBName2ID(ctx, dbModify.DbName); err != nil {
		return nil, err
	}
	bs, err := mc.Get(ctx, entity.DBKeyBody(id))
	errutil.ThrowError(err)
	if bs == nil {
		return nil, vearchpb.NewError(vearchpb.ErrorEnum_DB_NOT_EXIST, nil)
	}
	err = json.Unmarshal(bs, db)
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
	_, _, bodyKey := mc.DBKeys(db.Id, db.Name)
	value, err := json.Marshal(db)
	errutil.ThrowError(err)
	err = mc.Put(ctx, bodyKey, value)
	return db, err
}

// get servers belong this db
func (s *DBService) DBServers(ctx context.Context, dbName string) (servers []*entity.Server, err error) {
	defer errutil.CatchError(&err)
	mc := s.client.Master()
	// get all servers
	servers, err = mc.QueryServers(ctx)
	errutil.ThrowError(err)

	db, err := s.QueryDB(ctx, dbName)
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

func (s *DBService) validatePS(ctx context.Context, psList []string) error {
	if len(psList) < 1 {
		return nil
	}
	servers, err := s.client.Master().QueryServers(ctx)
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

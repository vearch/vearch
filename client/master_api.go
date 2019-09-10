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
	"github.com/spf13/cast"
	"github.com/tiglabs/log"
	"github.com/vearch/vearch/proto"
	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/util"
	"github.com/vearch/vearch/util/cbjson"
	"github.com/vearch/vearch/util/netutil"
	"net/http"
	"net/url"
	"time"
)

const Authorization = "Authorization"
const Root = "root"

func (this *masterClient) CreateDb(ctx context.Context, dbName string) error {
	data := make(map[string]interface{})
	data["name"] = dbName
	reqBody, err := cbjson.Marshal(data)

	masterServer.reset()
	var response []byte
	for {
		keyNumber, err := masterServer.getKey()
		if err != nil {
			return err
		}

		query := netutil.NewQuery().SetHeader(Authorization, util.AuthEncrypt(Root, this.cfg.Global.Signkey))
		query.SetMethod(http.MethodPut)
		query.SetAddress(this.cfg.Masters[keyNumber].ApiUrl())
		query.SetUrlPath("/db/_create")
		query.SetReqBody(string(reqBody))
		query.SetContentTypeJson()
		query.SetTimeout(60)
		log.Debug("master api create db url: %s, req body: %s", query.GetUrl(), string(reqBody))
		response, err = query.Do()
		log.Debug("master api create db response: %v", string(response))
		if err == nil {
			break
		}
		log.Debug("master api create db err: %v", err)

		masterServer.next()
	}

	jsonMap, err := cbjson.ByteToJsonMap(response)
	if err != nil {
		return err
	}

	code, err := jsonMap.GetJsonValIntE("code")
	if err != nil {
		return fmt.Errorf("client master api create db parse response code error: %s", err.Error())
	}

	if code != pkg.ERRCODE_SUCCESS {
		return fmt.Errorf("client master api create db parse response error, code: %d, msg: %s", code, jsonMap.GetJsonValStringOrDefault("msg", ""))
	}

	return nil
}

func (this *masterClient) CreateSpace(ctx context.Context, dbName string, space *entity.Space) error {
	reqBody, err := cbjson.Marshal(space)
	if err != nil {
		return err
	}

	masterServer.reset()
	var response []byte
	for {
		keyNumber, err := masterServer.getKey()
		if err != nil {
			return err
		}

		query := netutil.NewQuery().SetHeader(Authorization, util.AuthEncrypt(Root, this.cfg.Global.Signkey))
		query.SetAddress(this.cfg.Masters[keyNumber].ApiUrl())
		query.SetMethod(http.MethodPut)
		query.SetUrlPath("/space/" + dbName + "/_create")
		query.SetReqBody(string(reqBody))
		query.SetContentTypeJson()
		query.SetTimeout(60)
		log.Debug("master api create space url: %s, req body: %s", query.GetUrl(), string(reqBody))
		response, err = query.Do()
		log.Debug("master api create space response: %v", string(response))
		if err == nil {
			break
		}
		log.Debug("master api create space err: %v", err)

		masterServer.next()
	}

	jsonMap, err := cbjson.ByteToJsonMap(response)
	if err != nil {
		return err
	}

	code, err := jsonMap.GetJsonValIntE("code")
	if err != nil {
		return fmt.Errorf("client master api create space parse response code error: %s", err.Error())
	}

	if code != pkg.ERRCODE_SUCCESS {
		return fmt.Errorf("client master api create space parse response error, code: %d, msg: %s", code, jsonMap.GetJsonValStringOrDefault("msg", ""))
	}

	return nil
}

func (this *masterClient) UpdateSpace(ctx context.Context, dbName string, space *entity.Space) error {
	reqBody, err := cbjson.Marshal(space)
	if err != nil {
		return err
	}

	masterServer.reset()
	var response []byte
	for {
		keyNumber, err := masterServer.getKey()
		if err != nil {
			return err
		}

		query := netutil.NewQuery().SetHeader(Authorization, util.AuthEncrypt(Root, this.cfg.Global.Signkey))
		query.SetAddress(this.cfg.Masters[keyNumber].ApiUrl())
		query.SetMethod(http.MethodPost)
		query.SetUrlPath("/space/" + dbName + "/" + space.Name)
		query.SetReqBody(string(reqBody))
		query.SetContentTypeJson()
		query.SetTimeout(60)
		log.Debug("master api update space url: %s, req body: %s", query.GetUrl(), string(reqBody))
		response, err = query.Do()
		log.Debug("master api update space response: %v", string(response))
		if err == nil {
			break
		}
		log.Debug("master api update space err: %v", err)

		masterServer.next()
	}

	jsonMap, err := cbjson.ByteToJsonMap(response)
	if err != nil {
		return err
	}

	code, err := jsonMap.GetJsonValIntE("code")
	if err != nil {
		return fmt.Errorf("client master api update space parse response code error: %s", err.Error())
	}

	if code != pkg.ERRCODE_SUCCESS {
		return fmt.Errorf("client master api update space parse response error, code: %d, msg: %s", code, jsonMap.GetJsonValStringOrDefault("msg", ""))
	}

	return nil
}

func (this *masterClient) DeleteSpace(ctx context.Context, dbName string, spaceName string) error {
	masterServer.reset()
	var response []byte
	for {
		keyNumber, err := masterServer.getKey()
		if err != nil {
			return err
		}

		query := netutil.NewQuery().SetHeader(Authorization, util.AuthEncrypt(Root, this.cfg.Global.Signkey))
		query.SetAddress(this.cfg.Masters[keyNumber].ApiUrl())
		query.SetMethod(http.MethodDelete)
		query.SetUrlPath("/" + dbName + "/" + spaceName)
		query.SetTimeout(60)
		log.Debug("master api delete space url: %s", query.GetUrl())
		response, err = query.Do()
		log.Debug("master api delete space response: %v", string(response))
		if err == nil {
			break
		}
		log.Debug("master api delete space err: %v", err)

		masterServer.next()
	}

	jsonMap, err := cbjson.ByteToJsonMap(response)
	if err != nil {
		return err
	}

	code, err := jsonMap.GetJsonValIntE("code")
	if err != nil {
		return fmt.Errorf("client master api delete space parse response code error: %s", err.Error())
	}

	if code != pkg.ERRCODE_SUCCESS {
		return fmt.Errorf("client master api delete space parse response error, code: %d, msg: %s", code, jsonMap.GetJsonValStringOrDefault("msg", ""))
	}

	return nil
}

func (this *masterClient) Register(ctx context.Context, clusterName string, nodeId entity.NodeID, timeout time.Duration) (server *entity.Server, err error) {
	form := url.Values{}
	form.Add("clusterName", clusterName)
	form.Add("nodeId", cast.ToString(nodeId))

	masterServer.reset()
	var response []byte
	for {
		keyNumber, err := masterServer.getKey()
		if err != nil {
			return nil, err
		}

		query := netutil.NewQuery().SetHeader(Authorization, util.AuthEncrypt(Root, this.cfg.Global.Signkey))
		query.SetAddress(this.cfg.Masters[keyNumber].ApiUrl())
		query.SetMethod(http.MethodPost)
		query.SetQuery(form.Encode())
		query.SetUrlPath("/register")
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

	jsonMap, err := cbjson.ByteToJsonMap(response)
	if err != nil {
		return nil, err
	}

	code, err := jsonMap.GetJsonValIntE("code")
	if err != nil {
		return nil, fmt.Errorf("client master api register parse response code error: %s", err.Error())
	}

	if code != pkg.ERRCODE_SUCCESS {
		return nil, fmt.Errorf("client master api register parse response error, code: %d, msg: %s", code, jsonMap.GetJsonValStringOrDefault("msg", ""))
	}
	data, err := jsonMap.GetJsonValBytes("data")
	if err != nil {
		return nil, fmt.Errorf("client master api register parse response data error: %s", err.Error())
	}

	server = &entity.Server{}
	if err := cbjson.Unmarshal(data, server); err != nil {
		return nil, err
	}

	return server, nil
}

func (this *masterClient) RegisterPartition(ctx context.Context, partition *entity.Partition) error {
	reqBody, err := cbjson.Marshal(partition)
	if err != nil {
		return err
	}

	masterServer.reset()
	var response []byte
	for {
		keyNumber, err := masterServer.getKey()
		if err != nil {
			return err
		}

		query := netutil.NewQuery().SetHeader(Authorization, util.AuthEncrypt(Root, this.cfg.Global.Signkey))
		query.SetAddress(this.cfg.Masters[keyNumber].ApiUrl())
		query.SetMethod(http.MethodPost)
		query.SetUrlPath("/register_partition")
		query.SetReqBody(string(reqBody))
		query.SetContentTypeJson()
		query.SetTimeout(60)
		log.Debug("master api register partition url: %s, req body: %s", query.GetUrl(), string(reqBody))
		response, err = query.Do()
		log.Debug("master api register partition response: %v", string(response))
		if err == nil {
			break
		}
		log.Debug("master api register partition err: %v", err)

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

	if code != pkg.ERRCODE_SUCCESS {
		return fmt.Errorf("client master api register partiton parse response error, code: %d, msg: %s", code, jsonMap.GetJsonValStringOrDefault("msg", ""))
	}

	return nil
}

func (this *masterClient) FrozenPartition(ctx context.Context, partitionID entity.PartitionID) error {
	masterServer.reset()
	var response []byte
	for {
		keyNumber, err := masterServer.getKey()
		if err != nil {
			return err
		}

		query := netutil.NewQuery().SetHeader(Authorization, util.AuthEncrypt(Root, this.cfg.Global.Signkey))
		query.SetAddress(this.cfg.Masters[keyNumber].ApiUrl())
		query.SetMethod(http.MethodPost)
		query.SetUrlPath("/partition/frozen/" + cast.ToString(partitionID))
		query.SetTimeout(60)
		log.Debug("master api frozen partition url: %s, req body: %s", query.GetUrl(), "")
		response, err = query.Do()
		log.Debug("master api frozen partition response: %v", string(response))
		if err == nil {
			break
		}
		log.Debug("master api frozen partition err: %v", err)

		masterServer.next()
	}

	jsonMap, err := cbjson.ByteToJsonMap(response)
	if err != nil {
		return err
	}

	code, err := jsonMap.GetJsonValIntE("code")
	if err != nil {
		return fmt.Errorf("client master api frozen partiton parse response code error: %s", err.Error())
	}

	if code != pkg.ERRCODE_SUCCESS {
		return fmt.Errorf("client master api frozen partiton parse response error, code: %d, msg: %s", code, jsonMap.GetJsonValStringOrDefault("msg", ""))
	}

	return nil
}

var masterServer = &MasterServer{}

type MasterServer struct {
	total     int
	keyNumber int
	tryTimes  int
}

func (this *MasterServer) init(total int) {
	this.total = total
}

func (this *MasterServer) reset() {
	this.tryTimes = 0
}

func (this *MasterServer) getKey() (int, error) {
	if this.tryTimes >= this.total {
		return 0, fmt.Errorf("master server all down")
	}

	return this.keyNumber, nil
}

func (this *MasterServer) next() {
	this.keyNumber += 1
	if (this.keyNumber + 1) > this.total {
		this.keyNumber = 0
	}

	this.tryTimes += 1
}

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
	"github.com/vearch/vearch/config"
	"github.com/vearch/vearch/master/store"
)

type Client struct {
	master *masterClient
	ps     *psClient
}

func NewClient(conf *config.Config) (client *Client, err error) {
	client = &Client{}
	err = client.initPsClient(conf)
	if err != nil {
		return nil, err
	}
	err = client.initMasterClient(conf)
	if err != nil {
		return nil, err
	}
	return client, err
}
func (client *Client) initPsClient(conf *config.Config) error {
	client.ps = &psClient{client: client}
	return nil
}

func (client *Client) initMasterClient(conf *config.Config) error {

	openStore, err := store.OpenStore("etcd", conf.Masters.ClientAddress())
	if err != nil {
		return err
	}

	client.master = &masterClient{client: client, Store: openStore, cfg: conf}
	masterServer.init(len(conf.Masters))
	return nil
}

func (client *Client) Master() *masterClient {
	return client.master
}

func (client *Client) PS() *psClient {
	return client.ps
}

func (client *Client) Stop() {
	client.master.Stop()
	client.ps.Stop()
}

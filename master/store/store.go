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

package store

import (
	"context"
	"fmt"
	"time"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
)

var storeFactories = make(map[string]InitFunc)

type DataVersion int64

type InitFunc func(serverAddr []string) (Store, error)

func Register(name string, initFunc InitFunc) {
	storeFactories[name] = initFunc
}

func OpenStore(implementation string, serverAddress []string) (Store, error) {
	s, ok := storeFactories[implementation]
	if !ok {
		return nil, fmt.Errorf("not supported %v store", implementation)
	}

	return s(serverAddress)
}

type Store interface {
	Put(ctx context.Context, key string, value []byte) error
	Create(ctx context.Context, key string, value []byte) error
	CreateWithTTL(ctx context.Context, key string, value []byte, ttl time.Duration) error
	KeepAlive(ctx context.Context, key string, value []byte, ttl time.Duration) (<-chan *clientv3.LeaseKeepAliveResponse, error)
	PutWithLeaseId(ctx context.Context, key string, value []byte, ttl time.Duration, leaseId clientv3.LeaseID) error
	Update(ctx context.Context, key string, value []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	PrefixScan(ctx context.Context, prefix string) ([][]byte, [][]byte, error)
	Delete(ctx context.Context, key string) error
	//Here we should not use the STM structure of etcd, but should define a data structure
	//equivalent to STM in the store, and then convert it in etcdstorage.go, on the one hand, the
	//decoupling of etcd and master logic, on the other hand Easy to extend other types of storage
	STM(ctx context.Context, apply func(stm concurrency.STM) error) error
	NewLock(ctx context.Context, key string, timeout time.Duration) *DistLock
	//it to generate increment unique id
	NewIDGenerate(ctx context.Context, key string, base int64, timeout time.Duration) (int64, error)
	WatchPrefix(ctx context.Context, key string) (clientv3.WatchChan, error)
}

type WatcherJob interface {
	Put(event *clientv3.Event)
	Delete(event *clientv3.Event)
	Start()
	Stop()
}

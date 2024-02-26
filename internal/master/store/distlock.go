// Copyright 2019 The Google Authors.
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

	"github.com/vearch/vearch/proto/entity"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

/*
 * DistLock is based on vitess lock.go
 */
type DistLock struct {
	path    string
	leaseID clientv3.LeaseID
	client  *clientv3.Client
	ttl     time.Duration
	ctx     context.Context
}

func NewDistLock(ctx context.Context, client *clientv3.Client, key string, timeout time.Duration) *DistLock {
	return &DistLock{
		path:   entity.PrefixLock + key,
		client: client,
		ttl:    timeout,
		ctx:    ctx,
	}
}

func (dl *DistLock) Lock() (err error) {
	err = dl.lock()
	if err != nil {
		return dl.Unlock()
	}
	return nil
}

func (dl *DistLock) TryLock() (bool, error) {
	if dl.ttl <= 0 {
		dl.ttl = 30 * time.Second
	}

	lease, err := dl.client.Grant(dl.ctx, int64(dl.ttl.Seconds()))
	if err != nil {
		return false, err
	}
	dl.leaseID = lease.ID
	_, revision, err := dl.newUniqueEphemeralKV(dl.ctx, dl.client, lease.ID, dl.path, "")
	if err != nil {
		dl.client.Revoke(dl.ctx, dl.leaseID)
		return false, err
	}

	opts := append(clientv3.WithLastRev(), clientv3.WithMaxModRev(revision-1))
	lastKey, err := dl.client.Get(dl.ctx, dl.path+"/", opts...)
	if err != nil {
		dl.client.Revoke(dl.ctx, dl.leaseID)
		return false, err
	}

	if len(lastKey.Kvs) == 0 {
		return true, nil
	}

	dl.client.Revoke(dl.ctx, dl.leaseID)
	return false, fmt.Errorf("lock hold by ohter process")
}

func (dl *DistLock) newUniqueEphemeralKV(ctx context.Context, cli *clientv3.Client, leaseID clientv3.LeaseID, nodePath string, contents string) (string, int64, error) {
	newKey := fmt.Sprintf("%v/%v", nodePath, leaseID)
	txnresp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(newKey), "=", 0)).
		Then(clientv3.OpPut(newKey, contents, clientv3.WithLease(leaseID))).
		Commit()
	if err != nil {
		if err == context.Canceled || err == context.DeadlineExceeded {
			cli.Delete(context.Background(), newKey)
		}
		return "", 0, err
	}
	if !txnresp.Succeeded {
		return "", 0, fmt.Errorf("key already exist")
	}
	return newKey, txnresp.Header.Revision, nil
}

func (dl *DistLock) lock() (err error) {
	if dl.ttl <= 0 {
		dl.ttl = 30 * time.Second
	}

	lease, err := dl.client.Grant(dl.ctx, int64(dl.ttl.Seconds()))
	if err != nil {
		return err
	}
	dl.leaseID = lease.ID
	_, revision, err := dl.newUniqueEphemeralKV(dl.ctx, dl.client, lease.ID, dl.path, "")
	if err != nil {
		return err
	}

	for {
		done, err := dl.waitOnLastRev(dl.ctx, dl.path, revision)
		if err != nil {
			dl.client.Revoke(context.Background(), lease.ID)
			return err
		}
		if done {
			return nil
		}
	}

}

func (dl *DistLock) waitOnLastRev(ctx context.Context, nodePath string, revision int64) (bool, error) {
	opts := append(clientv3.WithLastRev(), clientv3.WithMaxModRev(revision-1))
	lastKey, err := dl.client.Get(ctx, nodePath+"/", opts...)
	if err != nil {
		return false, err
	}
	if len(lastKey.Kvs) == 0 {
		return true, nil
	}

	key := string(lastKey.Kvs[0].Key)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wc := dl.client.Watch(ctx, key, clientv3.WithRev(revision))
	if wc == nil {
		return false, fmt.Errorf("Watch failed")
	}

	lastKey, err = dl.client.Get(ctx, nodePath+"/", opts...)
	if err != nil {
		return false, err
	}
	if len(lastKey.Kvs) == 0 {
		return true, nil
	}

	select {
	case <-ctx.Done():
		return false, fmt.Errorf("wait on last revision ctx timeout")
	case wresp := <-wc:
		for _, ev := range wresp.Events {
			if ev.Type == mvccpb.DELETE {
				return false, nil
			}
		}
		return false, nil
	}
}

func (dl *DistLock) KeepAliveOnce() {
	dl.client.KeepAliveOnce(dl.ctx, dl.leaseID)
}

func (dl *DistLock) Unlock() error {
	_, err := dl.client.Revoke(dl.ctx, dl.leaseID)
	if err != nil {
		return fmt.Errorf("revoke lease %v error :%v", dl.leaseID, err)
	}
	return nil
}

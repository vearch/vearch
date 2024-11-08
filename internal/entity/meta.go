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

package entity

import (
	"fmt"
	"strings"
)

func LockSpaceKey(db, space string) string {
	return fmt.Sprintf("%s/%s/%s", PrefixLock, db, space)
}

func LockDBKey(db string) string {
	return fmt.Sprintf("%s/%s", PrefixLock, db)
}

func ServerKey(name NodeID) string {
	return fmt.Sprintf("%s%d", PrefixServer, name)
}

func SpaceKey(dbID, spaceId int64) string {
	return fmt.Sprintf("%s%d/%d", PrefixSpace, dbID, spaceId)
}

func SpaceConfigKey(dbID, spaceId int64) string {
	return fmt.Sprintf("%s%d/%d", PrefixSpaceConfig, dbID, spaceId)
}

func PartitionKey(partitionID uint32) string {
	return fmt.Sprintf("%s%d", PrefixPartition, partitionID)
}

func DBKeyId(id int64) string {
	return fmt.Sprintf("%sid/%d", PrefixDataBase, id)
}

func DBKeyName(name string) string {
	return fmt.Sprintf("%sname/%s", PrefixDataBase, name)
}

func DBKeyBody(id int64) string {
	return fmt.Sprintf("%sbody/%d", PrefixDataBase, id)
}

func UserKey(username string) string {
	return fmt.Sprintf("%s%s", PrefixUser, username)
}

func LockUserKey(username string) string {
	return fmt.Sprintf("%s%s", PrefixLock, username)
}

func RoleKey(rolename string) string {
	return fmt.Sprintf("%s%s", PrefixRole, rolename)
}

func LockRoleKey(rolename string) string {
	return fmt.Sprintf("%s%s", PrefixLock, rolename)
}

// FailServerKey generate fail server key
func FailServerKey(nodeID uint64) string {
	return fmt.Sprintf("%s%d", PrefixFailServer, nodeID)
}

// RouterKey Router key
func RouterKey(key, value string) string {
	return fmt.Sprintf("%s%s/%s", PrefixRouter, key, value)
}

func AliasKey(aliasName string) string {
	return fmt.Sprintf("%s%s", PrefixAlias, aliasName)
}

func LockAliasKey(aliasName string) string {
	return fmt.Sprintf("%s%s", PrefixLock, aliasName)
}

func SetPrefixAndSequence(cluster_id string) {
	if strings.HasPrefix(cluster_id, Prefix) {
		PrefixEtcdClusterID = cluster_id
	} else {
		PrefixEtcdClusterID = Prefix + cluster_id
	}
	NodeIdSequence = PrefixEtcdClusterID + PrefixNodeId
	SpaceIdSequence = PrefixEtcdClusterID + PrefixSpaceId
	DBIdSequence = PrefixEtcdClusterID + PrefixDBId
	PartitionIdSequence = PrefixEtcdClusterID + PrefixPartitionId

	PrefixUser = PrefixEtcdClusterID + PrefixUser
	PrefixLock = PrefixEtcdClusterID + PrefixLock
	PrefixLockCluster = PrefixEtcdClusterID + PrefixLockCluster
	PrefixServer = PrefixEtcdClusterID + PrefixServer
	PrefixSpace = PrefixEtcdClusterID + PrefixSpace
	PrefixSpaceConfig = PrefixEtcdClusterID + PrefixSpaceConfig
	PrefixPartition = PrefixEtcdClusterID + PrefixPartition
	PrefixDataBase = PrefixEtcdClusterID + PrefixDataBase
	PrefixDataBaseBody = PrefixEtcdClusterID + PrefixDataBaseBody
	PrefixFailServer = PrefixEtcdClusterID + PrefixFailServer
	PrefixRouter = PrefixEtcdClusterID + PrefixRouter
	PrefixAlias = PrefixEtcdClusterID + PrefixAlias
	PrefixRole = PrefixEtcdClusterID + PrefixRole
}

// sids sequence key for etcd
var (
	NodeIdSequence      = "/id/node"
	SpaceIdSequence     = "/id/space"
	DBIdSequence        = "/id/db"
	PartitionIdSequence = "/id/partition"
)

var (
	Prefix             = "/"
	PrefixUser         = "/user/"
	PrefixLock         = "/lock/"
	PrefixLockCluster  = "/lock/cluster"
	PrefixServer       = "/server/"
	PrefixSpace        = "/space/"
	PrefixSpaceConfig  = "/space_config/"
	PrefixPartition    = "/partition/"
	PrefixDataBase     = "/db/"
	PrefixDataBaseBody = "/db/body/"
	PrefixFailServer   = "/fail/server/"
	PrefixRouter       = "/router/"
	PrefixNodeId       = "/id/node"
	PrefixSpaceId      = "/id/space"
	PrefixDBId         = "/id/db"
	PrefixPartitionId  = "/id/partition"
	PrefixAlias        = "/alias/"
	PrefixRole         = "/role/"
)

var PrefixEtcdClusterID = "/vearch/default/"

// when master running clean job , it will set value to this key,
// when other got key , now time less than this they will skip this job
const ClusterCleanJobKey = "/cluster/cleanjob"

// ClusterWatchServerKey for server job lock
const ClusterWatchServerKey = "watch/server"

// rpc time out, default 10 * 1000 ms
type CTX_KEY string

var (
	RPC_TIME_OUT CTX_KEY = "rpc_timeout"
	MessageID            = "messageID"
)

type (
	// DBID is a custom type for database ID
	DBID = int64
	// SpaceID is a custom type for space ID
	SpaceID = int64
	// PartitionID is a custom type for partition ID
	PartitionID = uint32
	// SlotID is a custom type for slot ID
	SlotID = uint32
	// Version is a custom type for Partition
	Version = uint64
	// node id for ps
	NodeID = uint64
)

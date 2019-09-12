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
)

func LockSpaceKey(db, space string) string {
	return fmt.Sprintf("%s/%s", db, space)
}

func ServerKey(name NodeID) string {
	return fmt.Sprintf("%s%d", PrefixServer, name)
}

func SpaceKey(dbID, spaceId int64) string {
	return fmt.Sprintf("%s%d/%d", PrefixSpace, dbID, spaceId)
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

//ids sequence key for etcd
const (
	NodeIdSequence      = "/id/node"
	SpaceIdSequence     = "/id/space"
	DBIdSequence        = "/id/db"
	PartitionIdSequence = "/id/partition"
)

const (
	PrefixUser         = "/user/"
	PrefixLock         = "/lock/"
	PrefixLockCluster  = "/lock/_cluster"
	PrefixServer       = "/server/"
	PrefixSpace        = "/space/"
	PrefixPartition    = "/partition/"
	PrefixDataBase     = "/db/"
	PrefixDataBaseBody = "/db/body/"
)

//when master runing clean job , it will set value to this key,
//when other got key , now time less than this they will skip this job
const ClusterCleanJobKey = "/cluster/cleanjob"

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
	//node id for ps
	NodeID = uint64
)

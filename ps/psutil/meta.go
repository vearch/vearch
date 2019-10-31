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

package psutil

import (
	"context"
	"fmt"
	"github.com/vearch/vearch/client"
	"github.com/vearch/vearch/util/cbjson"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"time"

	"github.com/vearch/vearch/util/log"
	"github.com/vearch/vearch/proto/entity"
)

const MetaFile = "server_meta.txt"

type meta struct {
	Id          entity.NodeID
	ClusterName string
}

func InitMeta(client *client.Client, cluster, dataPath string) entity.NodeID {

	//first you need create dir
	if err := os.MkdirAll(dataPath, os.ModePerm); err != nil {
		panic(err)
	}

	var nodeID entity.NodeID

	metaPath := filepath.Join(dataPath, MetaFile)

	_, err := os.Stat(metaPath)

	if err != nil {
		if os.IsNotExist(err) {
			log.Info("Server create meta to file is: %v", metaPath)
			nodeID = createMeta(client, cluster, metaPath)
		} else {
			panic(err)
		}
	} else {
		log.Info("Server load meta from file is: %v", metaPath)
		nodeID = readMeta(cluster, metaPath)
	}

	if nodeID == 0 {
		panic("err name for node please check file path: " + metaPath)
	}

	return nodeID
}

//getInfo is used to load the persistence MetaInfo stored is the disk,and return the MetaInfo
func readMeta(cluster, metaPath string) entity.NodeID {
	if b, err := ioutil.ReadFile(metaPath); err == nil {
		temp := &meta{}
		if err := cbjson.Unmarshal(b, temp); err != nil {
			panic(err)
		}

		if temp.ClusterName != cluster {
			panic(fmt.Errorf("cluster name not same %s / %s, please check your conn master is right", cluster, temp.ClusterName))
		}

		return temp.Id
	} else {
		panic(err)
	}
}

func createMeta(client *client.Client, cluster, metaPath string) entity.NodeID {
	id, err := client.Master().NewIDGenerate(context.Background(), entity.NodeIdSequence, 1, 3*time.Second)
	if err != nil {
		panic(err)
	}

	temp := meta{ClusterName: cluster, Id: entity.NodeID(id)}
	bytes, err := cbjson.Marshal(temp)
	if err != nil {
		panic(err)
	}

	if err := ioutil.WriteFile(metaPath, bytes, os.ModePerm); err != nil {
		panic(err)
	}
	return entity.NodeID(id)

}

func GetAllPartitions(datas []string) []entity.PartitionID {
	ids := make(map[string]struct{}, 64)

	for _, data := range datas {
		if dir, err := ioutil.ReadDir(filepath.Join(data, "meta")); err == nil {
			for _, fi := range dir {
				if fi.IsDir() {
					ids[fi.Name()] = struct{}{}
				}
			}
		}
	}

	retVal := make([]entity.PartitionID, 0, len(ids))
	for id := range ids {
		if v, err := strconv.ParseUint(id, 10, 64); err == nil {
			retVal = append(retVal, uint32(v))
		}
	}

	return retVal
}

// path: {store_path}/{type}/{partition_id}
func partitionPath(data string, id entity.PartitionID, pathType string) string {
	return filepath.Join(data, pathType, fmt.Sprintf("%d", id))
}

func CreatePartitionPaths(dataPath string, space *entity.Space, id entity.PartitionID) (data, raft, meta string, err error) {

	data, raft, meta = GetPartitionPaths(dataPath, id)

	if err = os.MkdirAll(data, os.ModePerm); err != nil {
		return
	}

	if err = os.MkdirAll(raft, os.ModePerm); err != nil {
		return
	}

	if err = os.MkdirAll(meta, os.ModePerm); err != nil {
		return
	}

	if err != nil {
		return
	}

	err = SavePartitionMeta(dataPath, id, space)
	if err != nil {
		return
	}

	return
}

func LoadPartitionMeta(dataPath string, id entity.PartitionID) (*entity.Space, error) {
	_, _, meta := GetPartitionPaths(dataPath, id)
	bytes, err := ioutil.ReadFile(path.Join(meta, "meta.txt"))
	if err != nil {
		return nil, err
	}

	space := &entity.Space{}

	err = cbjson.Unmarshal(bytes, space)
	if err != nil {
		return nil, err
	}
	return space, nil
}

func SavePartitionMeta(dataPath string, id entity.PartitionID, space *entity.Space) error {
	_, _, meta := GetPartitionPaths(dataPath, id)
	bytes, err := cbjson.Marshal(space)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(path.Join(meta, "meta.txt"), bytes, os.ModePerm)
}

func GetPartitionPaths(path string, id entity.PartitionID) (data, raft, meta string) {
	data = partitionPath(path, id, "data")
	raft = partitionPath(path, id, "raft")
	meta = partitionPath(path, id, "meta")
	return
}

func ClearPartition(path string, id entity.PartitionID) {

	data, raft, meta := GetPartitionPaths(path, id)

	if err := os.RemoveAll(data); err != nil {
		log.Error("remove data , path:%s , err :%s", partitionPath(path, id, "data"), err.Error())
	}

	if err := os.RemoveAll(raft); err != nil {
		log.Error("remove raft , path:%s , err :%s", partitionPath(path, id, "raft"), err.Error())
	}

	if err := os.RemoveAll(meta); err != nil {
		log.Error("remove meta , path:%s , err :%s", partitionPath(path, id, "meta"), err.Error())
	}
}

func ClearAllPartition(path string) {
	if err := os.Remove(path); err != nil {
		log.Error("remove data , path:%s , err :%s", path, err.Error())
	}
}

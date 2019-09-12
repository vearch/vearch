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

package raftstore

//import (
//	"io"
//
//	"github.com/vearch/vearch/ps/engine"
//	"github.com/vearch/vearch/proto/metapb"
//	"github.com/vearch/vearch/proto/pspb/raftpb"
//	raftproto "github.com/tiglabs/raft/proto"
//)
//
//var (
//	snapMetaKey = []byte("raftSnapMeta")
//)
//
//// RaftSnapshot for raft snapshot duplicate
//type RaftSnapshot struct {
//	init     bool
//	replicas []metapb.Replica
//	snap     engine.Snapshot
//	snapIter engine.Iterator
//}
//
//// NewRaftSnapshot create a RaftSnapshot object
//func NewRaftSnapshot(snap engine.Snapshot, replicas []metapb.Replica) *RaftSnapshot {
//	return &RaftSnapshot{snap: snap, snapIter: snap.NewIterator()}
//}
//
//// Next read the next data from the snapshot
//func (s *RaftSnapshot) Next() (val []byte, err error) {
//	if !s.init {
//		s.init = true
//		snapMeta := &raftpb.SnapMeta{Replicas: s.replicas}
//		kvPair := raftpb.CreateSnapData()
//		kvPair.Key = snapMetaKey
//		if kvPair.Value, err = snapMeta.Marshal(); err == nil {
//			val, err = kvPair.Marshal()
//		}
//		kvPair.Close()
//		return
//	}
//
//	s.snapIter.Next()
//	if !s.snapIter.Valid() {
//		err = io.EOF
//		return
//	}
//
//	kvPair := raftpb.CreateSnapData()
//	kvPair.Key = s.snapIter.Key()
//	kvPair.Value = s.snapIter.Value()
//	val, err = kvPair.Marshal()
//	kvPair.Close()
//	return
//}
//
//// ApplyIndex return the raft applyIndex of snapshot
//func (s *RaftSnapshot) ApplyIndex() uint64 {
//	index, _ := s.snap.GetSN()
//	return index
//}
//
//// Close close snapshot
//func (s *RaftSnapshot) Close() {
//	s.snapIter.Close()
//	s.snap.Close()
//}
//
//// RaftSnapshotIterator for raft snapshot iterator
//type RaftSnapshotIterator struct {
//	snapIter raftproto.SnapIterator
//
//	valid bool
//	key   metapb.Key
//	val   metapb.Value
//}
//
//// NewRaftSnapshotIterator create a RaftSnapshotIterator object
//func NewRaftSnapshotIterator(iter raftproto.SnapIterator) *RaftSnapshotIterator {
//	return &RaftSnapshotIterator{snapIter: iter, valid: true}
//}
//
//// Next read the next data from the snapshot
//func (s *RaftSnapshotIterator) Next() {
//	if !s.valid {
//		return
//	}
//
//	data, err := s.snapIter.Next()
//	if err != nil {
//		s.valid = false
//		return
//	}
//
//	kvPair := raftpb.CreateSnapData()
//	err = kvPair.Unmarshal(data)
//	if err != nil {
//		s.valid = false
//		return
//	}
//
//	s.valid = true
//	s.key = kvPair.Key
//	s.val = kvPair.Value
//	kvPair.Close()
//}
//
//// Valid return the iterator is valid
//func (s *RaftSnapshotIterator) Valid() bool {
//	return s.valid
//}
//
//// Key return the current key
//func (s *RaftSnapshotIterator) Key() []byte {
//	return s.key
//}
//
//// Value return the current value
//func (s *RaftSnapshotIterator) Value() []byte {
//	return s.val
//}
//
//// Close close snapshot iterator
//func (s *RaftSnapshotIterator) Close() error {
//	s.valid = false
//	return nil
//}
//
//func (s *RaftSnapshotIterator) getSnapReplicas() ([]metapb.Replica, error) {
//	data, err := s.snapIter.Next()
//	if err != nil {
//		s.valid = false
//		return nil, err
//	}
//
//	snapMeta := new(raftpb.SnapMeta)
//	err = snapMeta.Unmarshal(data)
//	if err != nil {
//		s.valid = false
//		return nil, err
//	}
//
//	return snapMeta.Replicas, nil
//}

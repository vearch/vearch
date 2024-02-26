package client

import (
	"sync"

	"github.com/vearch/vearch/proto/entity"
	"github.com/vearch/vearch/internal/util/atomic"
)

type ReplicaRoundRobin struct {
	// key: partition id, value: atomic counter
	counterMap sync.Map
}

func NewReplicaRoundRobin() *ReplicaRoundRobin {
	return &ReplicaRoundRobin{}
}

func (rr *ReplicaRoundRobin) Next(pid entity.PartitionID, replicas []entity.NodeID) entity.NodeID {
	if len(replicas) <= 0 {
		return 0
	}

	var ix uint64
	counter, ok := rr.counterMap.LoadOrStore(pid, atomic.NewCounter(1))
	if ok {
		// loaded
		newValue := counter.(*atomic.AtomicCounter).Incr()
		ix = (newValue - 1) % uint64(len(replicas))
	}
	return replicas[ix]
}

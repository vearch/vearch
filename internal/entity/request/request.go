package request

import (
	"container/list"
	"context"
	"sync"

	"github.com/vearch/vearch/v3/internal/entity"
	"github.com/vearch/vearch/v3/internal/proto/vearchpb"
)

var (
	Rqueue = &RequestQueue{}
)

const (
	UnspecifiedStatus int32 = iota
	Running_1
	Running_2
	Canceled
	MemoryExceeded
)

type RequestId struct {
	MessageId   string
	PartitionId entity.PartitionID
}

type RequestQueue struct {
	Mutex   sync.RWMutex
	ReqMap  map[RequestId]*list.Element
	ReqList *list.List
}

type RequestStatus struct {
	Req        *vearchpb.PartitionData
	Status     int32
	CancelFunc context.CancelFunc
}

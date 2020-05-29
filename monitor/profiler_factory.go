package monitor

import (
	"github.com/caio/go-tdigest"
	"sync"
	"time"
)

//type KeyHistogram string
//const (
//	Query   KeyHistogram = "query"
//	Operate KeyHistogram = "operate"
//)
var mutex sync.Mutex

var metricMap = map[string]*Digest{}

func Profiler(key string, startNanosecond int) {
	mutex.Lock()
	digest, ok := metricMap[key]
	if ok == false {
		digest = NewDigest(key, 0.0)
		metricMap[key] = digest
	}

	mutex.Unlock()

	digest.Lock()
	costTime := (time.Now().Nanosecond() - startNanosecond) / 1000000
	//fmt.Printf("start:%v end:%v cost = %v   \n", startNanosecond, time.Now().Nanosecond(), costTime/1000000)
	digest.Digest.Add(float64(costTime))
	digest.Sum = digest.Sum + float64(costTime)

	digest.Unlock()
}

func SliceMetric() map[string]*Digest {
	mutex.Lock()
	newMap := metricMap
	metricMap = make(map[string]*Digest)
	mutex.Unlock()

	return newMap
}

type Digest struct {
	Name   string
	Sum    float64
	Digest tdigest.TDigest
	sync.Mutex
}

func NewDigest(Name string, Sum float64) *Digest {
	digest, _ := tdigest.New(tdigest.Compression(100))
	h := &Digest{
		Name:   Name,
		Sum:    Sum,
		Digest: *digest,
	}
	return h
}

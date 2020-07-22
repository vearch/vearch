package pool

import (
	"sync/atomic"
)

// ShardPool is a pool with sharding.
type ShardPool struct {
	getI  int64
	putI  int64
	shard int64
	pools []*Pool
}

// NewShardPool creates a new ShardPool with initilized sharded pools.
func NewShardPool(shard int, newFunc func() interface{}) *ShardPool {
	p := &ShardPool{shard: int64(shard)}
	p.pools = make([]*Pool, shard, shard)
	for i := 0; i < shard; i++ {
		p.pools[i] = &Pool{New: newFunc}
	}
	return p
}

// Get returns an item in pool.
func (p *ShardPool) Get() interface{} {
	i := atomic.AddInt64(&p.getI, 1) % p.shard
	return p.pools[i].Get()
}

// Put adds an item in pool.
func (p *ShardPool) Put(v interface{}) {
	i := atomic.AddInt64(&p.putI, 1) % p.shard
	p.pools[i].Put(v)
}

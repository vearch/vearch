package pool

// ChanPool is a pool implemented by channel.
type ChanPool struct {
	pool chan interface{}
	New  func() interface{}
}

// NewChanPool creates a new pool of objects.
func NewChanPool(max int) *ChanPool {
	return &ChanPool{
		pool: make(chan interface{}, max),
	}
}

// Get a object from the pool.
func (p *ChanPool) Get() interface{} {
	var c interface{}
	select {
	case c = <-p.pool:
	default:
		c = p.New()
	}
	return c
}

// Put returns a object to the pool.
func (p *ChanPool) Put(c interface{}) {
	select {
	case p.pool <- c:
	default:
		// let it go, let it go...
	}
}

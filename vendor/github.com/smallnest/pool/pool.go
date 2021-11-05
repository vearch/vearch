package pool

import (
	"sync"
)

// Pool is an object pool by linked list.
type Pool struct {
	l    int64
	head *entry
	mu   sync.Mutex
	// New optionally specifies a function to generate
	// a value when Get would otherwise return nil.
	// It may not be changed concurrently with calls to Get.
	New         func() interface{}
	freeEntries *entry
}

type entry struct {
	v    interface{}
	next *entry
}

func (p *Pool) getFreeEntry() *entry {
	e := p.freeEntries
	if e == nil {
		e = new(entry)
	} else {
		p.freeEntries = e.next
	}

	return e
}

func (p *Pool) putFreeEntry(e *entry) {
	e.v = nil
	e.next = p.freeEntries
	p.freeEntries = e
}

// Get selects an arbitrary item from the Pool, removes it from the Pool.
// If Get would otherwise return nil and p.New is non-nil, Get returns the result of calling p.New.
func (p *Pool) Get() interface{} {
	p.mu.Lock()
	e := p.head
	if e == nil {
		p.mu.Unlock()
		if p.New == nil {
			return nil
		}
		return p.New()
	}
	p.l--
	p.head = e.next

	v := e.v
	p.putFreeEntry(e)
	p.mu.Unlock()
	return v
}

// Put adds v to the pool.
func (p *Pool) Put(v interface{}) {
	p.mu.Lock()
	p.l++
	e := p.getFreeEntry()
	e.v = v
	e.next = p.head
	p.head = e
	p.mu.Unlock()
}

// Len returns the number of items in pool.
func (p *Pool) Len() int64 {
	var l int64
	p.mu.Lock()
	l = p.l
	p.mu.Unlock()
	return l
}

// Reset drops all items in pool.
func (p *Pool) Reset() {
	p.mu.Lock()
	p.head = nil
	p.freeEntries = nil
	p.l = 0
	p.mu.Unlock()
}

// Range iterates all items in pool. Notice it locks the pool in case of ranging.
func (p *Pool) Range(f func(v interface{}) bool) {
	p.mu.Lock()

	h := p.head
	for h != nil {
		if ok := f(h.v); !ok {
			break
		}
		h = h.next
	}
	p.mu.Unlock()
}

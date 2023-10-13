package goxstreams

import (
	"sync"
)

type pools struct {
	rawMessagePool   *rawMessagePool
	stringPool       *stringPool
	stringIntMapPool *stringIntMapPool
}

func newPools(size int) *pools {
	return &pools{
		rawMessagePool:   newRawMessagePool(size),
		stringPool:       newStringPool(size),
		stringIntMapPool: newStringIntMapPool(size),
	}
}

func (p *pools) xMessageGet() *[]xRawMessage {
	return p.rawMessagePool.get()
}

func (p *pools) xMessagePut(ptr *[]xRawMessage) {
	p.rawMessagePool.put(ptr)
}

func (p *pools) stringGet() *[]string {
	return p.stringPool.get()
}

func (p *pools) stringPut(ptr *[]string) {
	p.stringPool.put(ptr)
}

func (p *pools) stringIntMapGet() *map[string]int64 {
	return p.stringIntMapPool.get()
}

func (p *pools) stringIntMapPut(ptr *map[string]int64) {
	p.stringIntMapPool.put(ptr)
}

type stringPool struct {
	pool sync.Pool
}

func newStringPool(size int) *stringPool {
	return &stringPool{
		pool: sync.Pool{
			New: func() interface{} {
				a := make([]string, 0, size)
				return &a
			},
		}}
}

func (b *stringPool) get() *[]string {
	ptr := b.pool.Get().(*[]string)
	buf := *ptr
	buf = buf[:0]
	*ptr = buf

	return ptr
}

func (b *stringPool) put(ptr *[]string) {
	b.pool.Put(ptr)
}

type stringIntMapPool struct {
	pool sync.Pool
}

func newStringIntMapPool(size int) *stringIntMapPool {
	return &stringIntMapPool{
		pool: sync.Pool{
			New: func() interface{} {
				a := make(map[string]int64, size)
				return &a
			},
		}}
}

func (b *stringIntMapPool) get() *map[string]int64 {
	ptr := b.pool.Get().(*map[string]int64)
	buf := *ptr
	clear(buf)
	*ptr = buf

	return ptr
}

func (b *stringIntMapPool) put(ptr *map[string]int64) {
	b.pool.Put(ptr)
}

type rawMessagePool struct {
	pool sync.Pool
}

func newRawMessagePool(size int) *rawMessagePool {
	return &rawMessagePool{
		pool: sync.Pool{
			New: func() interface{} {
				a := make([]xRawMessage, 0, size)
				return &a
			},
		}}
}

func (b *rawMessagePool) get() *[]xRawMessage {
	ptr := b.pool.Get().(*[]xRawMessage)
	buf := *ptr
	buf = buf[:0]
	*ptr = buf

	return ptr
}

func (b *rawMessagePool) put(ptr *[]xRawMessage) {
	b.pool.Put(ptr)
}

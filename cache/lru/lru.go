package lru

import (
	"reflect"
	"sync"

	"github.com/golang/groupcache/lru"
	"github.com/renproject/kv/db"
)

type lruTable struct {
	mu    *sync.Mutex
	lru   *lru.Cache
	table db.Table
}

// NewLruTable return a lru cached table of the given table.
func NewLruTable(table db.Table, maxEntries int) db.Table {
	return &lruTable{
		mu:    new(sync.Mutex),
		lru:   lru.New(maxEntries),
		table: table,
	}
}

// Insert implements the `table` interface.
func (table *lruTable) Insert(key string, value interface{}) error {
	table.mutexLru(func(cache *lru.Cache) {
		cache.Add(key, value)
	})

	return table.table.Insert(key, value)
}

// Get implements the `table` interface.
func (table *lruTable) Get(key string, value interface{}) error {
	var val interface{}
	var ok bool
	table.mutexLru(func(cache *lru.Cache) {
		val, ok = cache.Get(key)
	})

	if ok {
		dest := reflect.ValueOf(value)
		if dest.Kind() == reflect.Ptr {
			ptrDest := dest.Elem()
			ptrDest.Set(reflect.ValueOf(val))
			return nil
		}
	}
	return table.table.Get(key, value)
}

// Delete implements the `table` interface.
func (table *lruTable) Delete(key string) error {
	table.mutexLru(func(cache *lru.Cache) {
		cache.Remove(key)
	})

	return table.table.Delete(key)
}

// Size implements the `table` interface.
func (table *lruTable) Size() (int, error) {
	// NOTE: It does not make sense to return the cache's len because the cache
	// might only have a subset of the actual data.
	return table.table.Size()
}

// Iterator implements the `table` interface.
func (table *lruTable) Iterator() db.Iterator {
	return table.table.Iterator()
}

// mutexLru takes a operation of the cache and lock/unlock the mutex before/after
// the operation to make it concurrent safe.
func (table *lruTable) mutexLru(operation func(*lru.Cache)) {
	table.mu.Lock()
	defer table.mu.Unlock()

	operation(table.lru)
}

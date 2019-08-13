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

func NewLruTable(table db.Table, maxEntries int) db.Table {
	return &lruTable{
		mu:    new(sync.Mutex),
		lru:   lru.New(maxEntries),
		table: table,
	}
}

func (table *lruTable) Insert(key string, value interface{}) error {
	table.mu.Lock()
	table.lru.Add(key, value)
	table.mu.Unlock()

	return table.table.Insert(key, value)
}

func (table *lruTable) Get(key string, value interface{}) error {
	table.mu.Lock()
	val, ok := table.lru.Get(key)
	table.mu.Unlock()

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

func (table *lruTable) Delete(key string) error {
	table.mu.Lock()
	table.lru.Remove(key)
	table.mu.Unlock()

	return table.table.Delete(key)
}

func (table *lruTable) Size() (int, error) {
	// NOTE: It does not make sense to return the cache's len because the cache
	// might only have a subset of the actual data.
	return table.table.Size()
}

func (table *lruTable) Iterator() (db.Iterator, error) {
	// NOTE: It does not make sense to return the cache's len because the cache
	// might only have a subset of the actual data.
	return table.table.Iterator()
}

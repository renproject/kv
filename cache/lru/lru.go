package lru

import (
	"reflect"
	"sync"

	"github.com/golang/groupcache/lru"
	"github.com/renproject/kv/db"
)

type inMemLRU struct {
	mu  *sync.Mutex
	lru *lru.Cache
	db  db.DB
}

func New(ldb db.DB, maxEntries int) db.DB {
	return &inMemLRU{
		mu:  new(sync.Mutex),
		lru: lru.New(maxEntries),
		db:  ldb,
	}
}

func (lruDB *inMemLRU) Insert(name string, key string, value interface{}) error {
	lruDB.mu.Lock()
	lruDB.lru.Add(key, value)
	lruDB.mu.Unlock()

	return lruDB.db.Insert(name, key, value)
}

func (lruDB *inMemLRU) Get(name string, key string, value interface{}) error {
	lruDB.mu.Lock()
	val, ok := lruDB.lru.Get(key)
	lruDB.mu.Unlock()

	if ok {
		dest := reflect.ValueOf(value)
		if dest.Kind() == reflect.Ptr {
			ptrDest := dest.Elem()
			ptrDest.Set(reflect.ValueOf(val))
			return nil
		}
	}
	return lruDB.db.Get(name, key, value)
}

func (lruDB *inMemLRU) Delete(name string, key string) error {
	lruDB.mu.Lock()
	lruDB.lru.Remove(key)
	lruDB.mu.Unlock()

	return lruDB.db.Delete(name, key)
}

func (lruDB *inMemLRU) Size(name string) (int, error) {
	return lruDB.db.Size(name)
}

func (lruDB *inMemLRU) Iterator(name string) (db.Iterator, error) {
	return lruDB.db.Iterator(name)
}

package lru

import (
	"reflect"

	"github.com/golang/groupcache/lru"
	"github.com/renproject/kv/db"
)

// table is a in-memory LRU implementation of the `db.Table`.
type table struct {
	lru     *lru.Cache
	dbTable db.Table
}

// NewTable returns a new table that wraps a `db.Table` along with an LRU cache.
func NewTable(dbTable db.Table, maxEntries int) db.Table {
	return &table{
		lru:     lru.New(maxEntries),
		dbTable: dbTable,
	}
}

// Insert implements the `db.Table` interface.
func (table *table) Insert(key string, value interface{}) error {
	if key == "" {
		return db.ErrEmptyKey
	}

	table.lru.Add(key, value)
	return table.dbTable.Insert(key, value)
}

// Get implements the `db.Table` interface.
func (table *table) Get(key string, value interface{}) error {
	if key == "" {
		return db.ErrEmptyKey
	}

	if val, ok := table.lru.Get(key); ok {
		dest := reflect.ValueOf(value)
		if dest.Kind() == reflect.Ptr {
			ptrDest := dest.Elem()
			ptrDest.Set(reflect.ValueOf(val))
			return nil
		}
	}
	return table.dbTable.Get(key, value)
}

// Delete implements the `db.Table` interface.
func (table *table) Delete(key string) error {
	if key == "" {
		return db.ErrEmptyKey
	}

	table.lru.Remove(key)
	return table.dbTable.Delete(key)
}

// Size implements the `db.Table` interface.
func (table *table) Size() (int, error) {
	// NOTE: It does not make sense to return the cache's len because the cache
	// might only have a subset of the actual data.
	return table.dbTable.Size()
}

// Iterator implements the `db.Table` interface.
func (table *table) Iterator() (db.Iterator, error) {
	// NOTE: It does not make sense to return the cache's iterator because the
	// cache might only have a subset of the actual data.
	return table.dbTable.Iterator()
}

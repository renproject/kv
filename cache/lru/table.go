package lru

import (
	"github.com/golang/groupcache/lru"
	"github.com/renproject/kv/db"
)

// table is a in-memory LRU implementation of the `db.Table`.
type table struct {
	lru     *lru.Cache
	dbTable db.Table
}

// New returns a new table.
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

	var ok bool
	value, ok = table.lru.Get(key)
	if ok {
		return nil
	}
	return table.dbTable.Get(key, value)
}

// Delete implements the `db.Table` interface.
func (table *table) Delete(key string) error {
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

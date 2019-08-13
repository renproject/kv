package lru

import (
	"sync"

	"github.com/renproject/kv/db"
)

type inMemLRU struct {
	tableMu *sync.Mutex
	tables  map[string]db.Table

	db         db.DB
	maxEntries int
}

// New returns a new lru DB which wraps the given db.
func New(ldb db.DB, maxEntries int) db.DB {
	return &inMemLRU{
		tableMu:    new(sync.Mutex),
		tables:     map[string]db.Table{},
		db:         ldb,
		maxEntries: maxEntries,
	}
}

func (lruDB *inMemLRU) Table(name string) db.Table {
	lruDB.tableMu.Lock()
	defer lruDB.tableMu.Unlock()

	table, ok := lruDB.tables[name]
	if !ok {
		table = NewLruTable(lruDB.db.Table(name), lruDB.maxEntries)
		lruDB.tables[name] = table
	}
	return table
}

// Insert implements the `db.DB` interface.
func (lruDB *inMemLRU) Insert(name string, key string, value interface{}) error {
	table := lruDB.Table(name)
	return table.Insert(key, value)
}

// Get implements the `db.DB` interface.
func (lruDB *inMemLRU) Get(name string, key string, value interface{}) error {
	table := lruDB.Table(name)
	return table.Get(key, value)
}

// Delete implements the `db.DB` interface.
func (lruDB *inMemLRU) Delete(name string, key string) error {
	table := lruDB.Table(name)
	return table.Delete(key)
}

// Size implements the `db.DB` interface.
func (lruDB *inMemLRU) Size(name string) (int, error) {
	table := lruDB.Table(name)
	return table.Size()
}

// Iterator implements the `db.DB` interface.
func (lruDB *inMemLRU) Iterator(name string) (db.Iterator, error) {
	table := lruDB.Table(name)
	return table.Iterator()
}

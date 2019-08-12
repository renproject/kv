package lru

import (
	"github.com/renproject/kv/db"
)

type inMemLRU struct {
	maxEntries int
	db         db.DB
	tables     map[string]db.Table
}

func New(ldb db.DB, maxEntriesPerTable int) db.DB {
	return &inMemLRU{
		maxEntries: maxEntriesPerTable,
		db:         ldb,
		tables:     map[string]db.Table{},
	}
}

func (lruDB *inMemLRU) Insert(name string, key string, value interface{}) error {
	return lruDB.db.Insert(name, key, value)
}

func (lruDB *inMemLRU) Get(name string, key string, value interface{}) error {
	return lruDB.db.Get(name, key, value)
}

func (lruDB *inMemLRU) Delete(name string, key string) error {
	return lruDB.db.Delete(name, key)
}

func (lruDB *inMemLRU) Size(name string) (int, error) {
	return lruDB.db.Size(name)
}

func (lruDB *inMemLRU) Iterator(name string) (db.Iterator, error) {
	return lruDB.db.Iterator(name)
}

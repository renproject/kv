package leveldb

import (
	"sync"

	"github.com/renproject/kv/db"
	"github.com/syndtr/goleveldb/leveldb"
)

// levelDB is a leveldb implementation of the `db.Iterable`.
type levelDB struct {
	mu     *sync.Mutex
	db     *leveldb.DB
	codec  db.Codec
	tables map[string]db.Table
}

// New returns a new `db.Iterable`.
func New(ldb *leveldb.DB, codec db.Codec) db.DB {
	return &levelDB{
		mu:     new(sync.Mutex),
		db:     ldb,
		codec:  codec,
		tables: map[string]db.Table{},
	}
}

// Insert implements the `db.DB` interface.
func (ldb *levelDB) Insert(name string, key string, value interface{}) error {
	table := ldb.table(name)
	return table.Insert(key, value)
}

// Get implements the `db.DB` interface.
func (ldb *levelDB) Get(name string, key string, value interface{}) error {
	table := ldb.table(name)
	return table.Get(key, value)
}

// Delete implements the `db.DB` interface.
func (ldb *levelDB) Delete(name string, key string) error {
	table := ldb.table(name)
	return table.Delete(key)
}

// Size implements the `db.DB` interface.
func (ldb *levelDB) Size(name string) (int, error) {
	table := ldb.table(name)
	return table.Size()
}

// Iterator implements the `db.DB` interface.
func (ldb *levelDB) Iterator(name string) (db.Iterator, error) {
	table := ldb.table(name)
	return table.Iterator()
}

// table implements the `db.DB` interface.
func (ldb *levelDB) table(name string) db.Table {
	ldb.mu.Lock()
	defer ldb.mu.Unlock()

	if table, ok := ldb.tables[name]; ok {
		return table
	}
	table := NewTable(name, ldb.db, ldb.codec)
	ldb.tables[name] = table
	return table
}

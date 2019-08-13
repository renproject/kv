package badgerdb

import (
	"sync"

	"github.com/dgraph-io/badger"
	"github.com/renproject/kv/db"
)

// badgerDB is a badgerDB implementation of the `db.Iterable`.
type badgerDB struct {
	mu     *sync.Mutex
	db     *badger.DB
	codec  db.Codec
	tables map[string]db.Table
}

// New returns a new `db.Iterable`.
func New(bdb *badger.DB, codec db.Codec) db.DB {
	return &badgerDB{
		mu:     new(sync.Mutex),
		db:     bdb,
		codec:  codec,
		tables: map[string]db.Table{},
	}
}

// Table implements the `db.DB` interface.
func (bdb *badgerDB) Table(name string) db.Table {
	bdb.mu.Lock()
	defer bdb.mu.Unlock()

	if table, ok := bdb.tables[name]; ok {
		return table
	}
	table := NewTable(name, bdb.db, bdb.codec)
	bdb.tables[name] = table
	return table
}

// Insert implements the `db.DB` interface.
func (bdb *badgerDB) Insert(name string, key string, value interface{}) error {
	table := bdb.Table(name)
	return table.Insert(key, value)
}

// Get implements the `db.DB` interface.
func (bdb *badgerDB) Get(name string, key string, value interface{}) error {
	table := bdb.Table(name)
	return table.Get(key, value)
}

// Delete implements the `db.DB` interface.
func (bdb *badgerDB) Delete(name string, key string) error {
	table := bdb.Table(name)
	return table.Delete(key)
}

// Size implements the `db.DB` interface.
func (bdb *badgerDB) Size(name string) (int, error) {
	table := bdb.Table(name)
	return table.Size()
}

// Iterator implements the `db.DB` interface.
func (bdb *badgerDB) Iterator(name string) (db.Iterator, error) {
	table := bdb.Table(name)
	return table.Iterator()
}

package badgerdb

import (
	"github.com/dgraph-io/badger"
	"github.com/renproject/kv/db"
)

// bdb is a badgerDB implementation of the `db.Iterable`.
type badgerDB struct {
	db    *badger.DB
	codec db.Codec
}

// New returns a new `db.Iterable`.
func New(bdb *badger.DB, codec db.Codec) db.DB {
	return &badgerDB{
		db:    bdb,
		codec: codec,
	}
}

// Insert implements the `db.DB` interface.
func (bdb *badgerDB) Insert(name string, key string, value interface{}) error {
	table := NewTable(name, bdb.db, bdb.codec)
	return table.Insert(key, value)
}

// Get implements the `db.DB` interface.
func (bdb *badgerDB) Get(name string, key string, value interface{}) error {
	table := NewTable(name, bdb.db, bdb.codec)
	return table.Get(key, value)
}

// Delete implements the `db.DB` interface.
func (bdb *badgerDB) Delete(name string, key string) error {
	table := NewTable(name, bdb.db, bdb.codec)
	return table.Delete(key)
}

// Size implements the `db.DB` interface.
func (bdb *badgerDB) Size(name string) (int, error) {
	table := NewTable(name, bdb.db, bdb.codec)
	return table.Size()
}

// Iterator implements the `db.DB` interface.
func (bdb *badgerDB) Iterator(name string) (db.Iterator, error) {
	table := NewTable(name, bdb.db, bdb.codec)
	return table.Iterator()
}

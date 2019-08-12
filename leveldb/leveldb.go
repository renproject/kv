package leveldb

import (
	"github.com/renproject/kv/db"
	"github.com/syndtr/goleveldb/leveldb"
)

// levelDB is a leveldb implementation of the `db.Iterable`.
type levelDB struct {
	db    *leveldb.DB
	codec db.Codec
}

// New returns a new `db.Iterable`.
func New(ldb *leveldb.DB, codec db.Codec) db.DB {
	return &levelDB{
		db:    ldb,
		codec: codec,
	}
}

// Insert implements the `db.DB` interface.
func (ldb *levelDB) Insert(name string, key string, value interface{}) error {
	table := NewTable(name, ldb.db, ldb.codec)
	return table.Insert(key, value)
}

// Get implements the `db.DB` interface.
func (ldb *levelDB) Get(name string, key string, value interface{}) error {
	table := NewTable(name, ldb.db, ldb.codec)
	return table.Get(key, value)
}

// Delete implements the `db.DB` interface.
func (ldb *levelDB) Delete(name string, key string) error {
	table := NewTable(name, ldb.db, ldb.codec)
	return table.Delete(key)
}

// Size implements the `db.DB` interface.
func (ldb *levelDB) Size(name string) (int, error) {
	table := NewTable(name, ldb.db, ldb.codec)
	return table.Size()
}

// Iterator implements the `db.DB` interface.
func (ldb *levelDB) Iterator(name string) (db.Iterator, error) {
	table := NewTable(name, ldb.db, ldb.codec)
	return table.Iterator()
}

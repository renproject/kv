package leveldb

import (
	"github.com/renproject/kv/db"
	"github.com/syndtr/goleveldb/leveldb"
)

// ldb is levelDB implementation of the `db.Iterable`
type ldb struct {
	db    *leveldb.DB
	codec db.Codec
}

// New returns a new ldb.
func New(leveldb *leveldb.DB, codec db.Codec) db.DB {
	return &ldb{
		db:    leveldb,
		codec: codec,
	}
}

// Insert implements the `db.DB` interface.
func (ldb *ldb) Insert(name string, key string, value interface{}) error {
	table := NewTable(name, ldb.db, ldb.codec)
	return table.Insert(key, value)
}

// Get implements the `db.DB` interface.
func (ldb *ldb) Get(name string, key string, value interface{}) error {
	table := NewTable(name, ldb.db, ldb.codec)
	return table.Get(key, value)
}

// Delete implements the `db.DB` interface.
func (ldb *ldb) Delete(name string, key string) error {
	table := NewTable(name, ldb.db, ldb.codec)
	return table.Delete(key)
}

// Size implements the `db.DB` interface.
func (ldb *ldb) Size(name string) (int, error) {
	table := NewTable(name, ldb.db, ldb.codec)
	return table.Size()
}

// Iterator implements the `db.DB` interface.
func (ldb *ldb) Iterator(name string) (db.Iterator, error) {
	table := NewTable(name, ldb.db, ldb.codec)
	return table.Iterator()
}

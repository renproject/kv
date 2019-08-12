package memdb

import (
	"sync"

	"github.com/renproject/kv/db"
)

// memdb is a in-memory implementation of the `db.DB`.
type memdb struct {
	mu     *sync.RWMutex
	tables map[string]db.Table
	codec  db.Codec
}

// New returns a new memdb.
func New(codec db.Codec) db.DB {
	return &memdb{
		mu:     new(sync.RWMutex),
		tables: map[string]db.Table{},
		codec:  codec,
	}
}

// Insert implements the `db.DB` interface.
func (memdb *memdb) Insert(name string, key string, value interface{}) error {
	table := memdb.table(name)
	return table.Insert(key, value)
}

// Get implements the `db.DB` interface.
func (memdb *memdb) Get(name string, key string, value interface{}) error {
	table := memdb.table(name)
	return table.Get(key, value)
}

// Delete implements the `db.DB` interface.
func (memdb *memdb) Delete(name string, key string) error {
	table := memdb.table(name)
	return table.Delete(key)
}

// Size implements the `db.DB` interface.
func (memdb *memdb) Size(name string) (int, error) {
	table := memdb.table(name)
	return table.Size()
}

// Iterator implements the `db.DB` interface.
func (memdb *memdb) Iterator(name string) (db.Iterator, error) {
	table := memdb.table(name)
	return table.Iterator()
}

func (memdb *memdb) table(name string) db.Table {
	memdb.mu.Lock()
	defer memdb.mu.Unlock()

	table, ok := memdb.tables[name]
	if !ok {
		table = NewTable(memdb.codec)
		memdb.tables[name] = table
	}
	return table
}

package memdb

import (
	"sync"

	"github.com/renproject/kv/db"
)

// memdb is a in-memory implementation of the `db.DB`.
type memdb struct {
	mu     *sync.RWMutex
	tables map[string]db.Table
}

// New returns a new memdb.
func New() db.DB {
	return &memdb{
		mu:     new(sync.RWMutex),
		tables: map[string]db.Table{},
	}
}

// NewTable implements the `db.DB` interface.
func (memdb *memdb) NewTable(name string, codec db.Codec) (db.Table, error) {
	memdb.mu.Lock()
	defer memdb.mu.Unlock()

	_, ok := memdb.tables[name]
	if ok {
		return nil, db.ErrTableAlreadyExists
	}
	memdb.tables[name] = NewTable(codec)
	return memdb.tables[name], nil
}

// Table implements the `db.DB` interface.
func (memdb *memdb) Table(name string) (db.Table, error) {
	memdb.mu.Lock()
	defer memdb.mu.Unlock()

	table, ok := memdb.tables[name]
	if !ok {
		return nil, db.ErrTableNotFound
	}

	return table, nil
}

// Insert implements the `db.DB` interface.
func (memdb *memdb) Insert(name string, key string, value interface{}) error {
	table, err := memdb.Table(name)
	if err != nil {
		return err
	}

	return table.Insert(key, value)
}

// Get implements the `db.DB` interface.
func (memdb *memdb) Get(name string, key string, value interface{}) error {
	table, err := memdb.Table(name)
	if err != nil {
		return err
	}

	return table.Get(key, value)
}

// Delete implements the `db.DB` interface.
func (memdb *memdb) Delete(name string, key string) error {
	table, err := memdb.Table(name)
	if err != nil {
		return err
	}

	return table.Delete(key)
}

// Size implements the `db.DB` interface.
func (memdb *memdb) Size(name string) (int, error) {
	table, err := memdb.Table(name)
	if err != nil {
		return 0, err
	}

	return table.Size()
}

// Iterator implements the `db.DB` interface.
func (memdb *memdb) Iterator(name string) (db.Iterator, error) {
	table, err := memdb.Table(name)
	if err != nil {
		return nil, err
	}

	return table.Iterator()
}

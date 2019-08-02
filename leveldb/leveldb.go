package leveldb

import (
	"sync"

	"github.com/renproject/kv/db"
	"github.com/syndtr/goleveldb/leveldb"
)

// ldb is levelDB implementation of the `db.Iterable`
type ldb struct {
	mu     *sync.Mutex
	db     *leveldb.DB
	tables map[string]db.Table
}

// New returns a new ldb.
func New(leveldb *leveldb.DB) db.DB {
	return &ldb{
		mu:     new(sync.Mutex),
		db:     leveldb,
		tables: map[string]db.Table{},
	}
}

// NewTable returns a badgerDB implementation of the db.Table.
func (ldb *ldb) NewTable(name string, codec db.Codec) (db.Table, error) {
	ldb.mu.Lock()
	defer ldb.mu.Unlock()

	_, ok := ldb.tables[name]
	if ok {
		return nil, db.ErrTableAlreadyExists
	}
	ldb.tables[name] = NewTable(name, ldb.db, codec)
	return ldb.tables[name], nil
}

// Table implements the `db.DB` interface.
func (ldb *ldb) Table(name string) (db.Table, error) {
	ldb.mu.Lock()
	defer ldb.mu.Unlock()

	table, ok := ldb.tables[name]
	if !ok {
		return nil, db.ErrTableNotFound
	}

	return table, nil
}

// Insert implements the `db.DB` interface.
func (ldb *ldb) Insert(name string, key string, value interface{}) error {
	table, err := ldb.Table(name)
	if err != nil {
		return err
	}

	return table.Insert(key, value)
}

// Get implements the `db.DB` interface.
func (ldb *ldb) Get(name string, key string, value interface{}) error {
	table, err := ldb.Table(name)
	if err != nil {
		return err
	}

	return table.Get(key, value)
}

// Delete implements the `db.DB` interface.
func (ldb *ldb) Delete(name string, key string) error {
	table, err := ldb.Table(name)
	if err != nil {
		return err
	}

	return table.Delete(key)
}

// Size implements the `db.DB` interface.
func (ldb *ldb) Size(name string) (int, error) {
	table, err := ldb.Table(name)
	if err != nil {
		return 0, err
	}

	return table.Size()
}

// Iterator implements the `db.DB` interface.
func (ldb *ldb) Iterator(name string) (db.Iterator, error) {
	table, err := ldb.Table(name)
	if err != nil {
		return nil, err
	}

	return table.Iterator()
}

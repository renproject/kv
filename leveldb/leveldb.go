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
	tables map[string]db.Table
}

// New returns a new `db.Iterable`.
func New(ldb *leveldb.DB) db.DB {
	return &levelDB{
		mu:     new(sync.Mutex),
		db:     ldb,
		tables: map[string]db.Table{},
	}
}

// NewTable returns a levelDB implementation of the db.Table.
func (ldb *levelDB) NewTable(name string, codec db.Codec) (db.Table, error) {
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
func (ldb *levelDB) Table(name string) (db.Table, error) {
	ldb.mu.Lock()
	defer ldb.mu.Unlock()

	table, ok := ldb.tables[name]
	if !ok {
		return nil, db.ErrTableNotFound
	}

	return table, nil
}

// Insert implements the `db.DB` interface.
func (ldb *levelDB) Insert(name string, key string, value interface{}) error {
	table, err := ldb.Table(name)
	if err != nil {
		return err
	}

	return table.Insert(key, value)
}

// Get implements the `db.DB` interface.
func (ldb *levelDB) Get(name string, key string, value interface{}) error {
	table, err := ldb.Table(name)
	if err != nil {
		return err
	}

	return table.Get(key, value)
}

// Delete implements the `db.DB` interface.
func (ldb *levelDB) Delete(name string, key string) error {
	table, err := ldb.Table(name)
	if err != nil {
		return err
	}

	return table.Delete(key)
}

// Size implements the `db.DB` interface.
func (ldb *levelDB) Size(name string) (int, error) {
	table, err := ldb.Table(name)
	if err != nil {
		return 0, err
	}

	return table.Size()
}

// Iterator implements the `db.DB` interface.
func (ldb *levelDB) Iterator(name string) (db.Iterator, error) {
	table, err := ldb.Table(name)
	if err != nil {
		return nil, err
	}

	return table.Iterator()
}

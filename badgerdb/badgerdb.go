package badgerdb

import (
	"sync"

	"github.com/dgraph-io/badger"
	"github.com/renproject/kv/db"
)

// bdb is a badgerDB implementation of the `db.Iterable`.
type badgerDB struct {
	mu     *sync.Mutex
	db     *badger.DB
	tables map[string]db.Table
}

// New returns a new `db.Iterable`.
func New(bdb *badger.DB) db.DB {
	return &badgerDB{
		mu:     new(sync.Mutex),
		db:     bdb,
		tables: map[string]db.Table{},
	}
}

// NewTable returns a badgerDB implementation of the db.Table.
func (bdb *badgerDB) NewTable(name string, codec db.Codec) (db.Table, error) {
	bdb.mu.Lock()
	defer bdb.mu.Unlock()

	_, ok := bdb.tables[name]
	if ok {
		return nil, db.ErrTableAlreadyExists
	}
	bdb.tables[name] = NewTable(name, bdb.db, codec)
	return bdb.tables[name], nil
}

// Table implements the `db.DB` interface.
func (bdb *badgerDB) Table(name string) (db.Table, error) {
	bdb.mu.Lock()
	defer bdb.mu.Unlock()

	table, ok := bdb.tables[name]
	if !ok {
		return nil, db.ErrTableNotFound
	}

	return table, nil
}

// Insert implements the `db.DB` interface.
func (bdb *badgerDB) Insert(name string, key string, value interface{}) error {
	table, err := bdb.Table(name)
	if err != nil {
		return err
	}

	return table.Insert(key, value)
}

// Get implements the `db.DB` interface.
func (bdb *badgerDB) Get(name string, key string, value interface{}) error {
	table, err := bdb.Table(name)
	if err != nil {
		return err
	}

	return table.Get(key, value)
}

// Delete implements the `db.DB` interface.
func (bdb *badgerDB) Delete(name string, key string) error {
	table, err := bdb.Table(name)
	if err != nil {
		return err
	}

	return table.Delete(key)
}

// Size implements the `db.DB` interface.
func (bdb *badgerDB) Size(name string) (int, error) {
	table, err := bdb.Table(name)
	if err != nil {
		return 0, err
	}

	return table.Size()
}

// Iterator implements the `db.DB` interface.
func (bdb *badgerDB) Iterator(name string) (db.Iterator, error) {
	table, err := bdb.Table(name)
	if err != nil {
		return nil, err
	}

	return table.Iterator()
}

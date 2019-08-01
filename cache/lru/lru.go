package lru

import (
	"github.com/renproject/kv/db"
)

type inMemLRU struct {
	maxEntries int
	db         db.DB
	tables     map[string]db.Table
}

func New(ldb db.DB, maxEntriesPerTable int) db.DB {
	return &inMemLRU{
		maxEntries: maxEntriesPerTable,
		db:         ldb,
		tables:     map[string]db.Table{},
	}
}

func (lruDB *inMemLRU) NewTable(name string, codec db.Codec) (db.Table, error) {
	_, ok := lruDB.tables[name]
	if ok {
		return nil, db.ErrTableAlreadyExists
	}
	memDB, err := lruDB.db.NewTable(name, codec)
	if err != nil {
		return nil, err
	}
	lruDB.tables[name] = NewTable(memDB, lruDB.maxEntries)
	return lruDB.tables[name], nil
}

func (lruDB *inMemLRU) Table(name string) (db.Table, error) {
	table, ok := lruDB.tables[name]
	if !ok {
		return nil, db.ErrTableNotFound
	}

	return table, nil
}

func (lruDB *inMemLRU) Insert(name string, key string, value interface{}) error {
	table, err := lruDB.Table(name)
	if err != nil {
		return err
	}

	return table.Insert(key, value)
}

func (lruDB *inMemLRU) Get(name string, key string, value interface{}) error {
	table, err := lruDB.Table(name)
	if err != nil {
		return err
	}

	return table.Get(key, value)
}

func (lruDB *inMemLRU) Delete(name string, key string) error {
	table, err := lruDB.Table(name)
	if err != nil {
		return err
	}

	return table.Delete(key)
}

func (lruDB *inMemLRU) Size(name string) (int, error) {
	table, err := lruDB.Table(name)
	if err != nil {
		return 0, err
	}

	return table.Size()
}

func (lruDB *inMemLRU) Iterator(name string) (db.Iterator, error) {
	table, err := lruDB.Table(name)
	if err != nil {
		return nil, err
	}

	return table.Iterator()
}

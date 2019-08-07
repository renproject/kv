package ttl

import (
	"time"

	"github.com/renproject/kv/db"
)

type inMemTTL struct {
	timeToLive time.Duration
	db         db.DB
	tables     map[string]db.Table
}

func New(ldb db.DB, timeToLive time.Duration) db.DB {
	return &inMemTTL{
		timeToLive: timeToLive,
		db:         ldb,
		tables:     map[string]db.Table{},
	}
}

func (ttlDB *inMemTTL) NewTable(name string, codec db.Codec) (db.Table, error) {
	_, ok := ttlDB.tables[name]
	if ok {
		return nil, db.ErrTableAlreadyExists
	}
	memDB, err := ttlDB.db.NewTable(name, codec)
	if err != nil {
		return nil, err
	}
	table, err := NewTable(memDB, ttlDB.timeToLive)
	if err != nil {
		return nil, err
	}
	ttlDB.tables[name] = table
	return ttlDB.tables[name], nil
}

func (ttlDB *inMemTTL) Table(name string) (db.Table, error) {
	table, ok := ttlDB.tables[name]
	if !ok {
		return nil, db.ErrTableNotFound
	}

	return table, nil
}

func (ttlDB *inMemTTL) Insert(name string, key string, value interface{}) error {
	table, err := ttlDB.Table(name)
	if err != nil {
		return err
	}

	return table.Insert(key, value)
}

func (ttlDB *inMemTTL) Get(name string, key string, value interface{}) error {
	table, err := ttlDB.Table(name)
	if err != nil {
		return err
	}

	return table.Get(key, value)
}

func (ttlDB *inMemTTL) Delete(name string, key string) error {
	table, err := ttlDB.Table(name)
	if err != nil {
		return err
	}

	return table.Delete(key)
}

func (ttlDB *inMemTTL) Size(name string) (int, error) {
	table, err := ttlDB.Table(name)
	if err != nil {
		return 0, err
	}

	return table.Size()
}

func (ttlDB *inMemTTL) Iterator(name string) (db.Iterator, error) {
	table, err := ttlDB.Table(name)
	if err != nil {
		return nil, err
	}

	return table.Iterator()
}

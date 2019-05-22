package leveldb

import (
	"github.com/renproject/kv/db"
	"github.com/syndtr/goleveldb/leveldb"
)

type ldb struct {
	db *leveldb.DB
}

func New(db *leveldb.DB) db.DB {
	return &ldb{
		db: db,
	}
}

func (ldb *ldb) Insert(key string, data []byte) error {
	return ldb.db.Put([]byte(key), data, nil)
}

func (ldb *ldb) Get(key string) (value []byte, err error) {
	value, err = ldb.db.Get([]byte(key), nil)
	if err == leveldb.ErrNotFound {
		err = db.ErrNotFound
	}
	return
}

func (ldb *ldb) Delete(key string) error {
	return ldb.db.Delete([]byte(key), nil)
}

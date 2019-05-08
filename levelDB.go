package store

import (
	"encoding/json"

	"github.com/syndtr/goleveldb/leveldb"
)

type ldb struct {
	db *leveldb.DB
}

func NewLevelDB(db *leveldb.DB) Store {
	return &ldb{
		db: db,
	}
}

func (db *ldb) Read(key string, value interface{}) error {
	data, err := db.db.Get([]byte(key), nil)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, value)
}

func (db *ldb) ReadData(key string) ([]byte, error) {
	return db.db.Get([]byte(key), nil)
}

func (db *ldb) Write(key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return db.db.Put([]byte(key), data, nil)
}

func (db *ldb) WriteData(key string, data []byte) error {
	return db.db.Put([]byte(key), data, nil)
}

func (db *ldb) Delete(key string) error {
	return db.db.Delete([]byte(key), nil)
}

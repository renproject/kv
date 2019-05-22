package leveldb

import (
	"encoding/json"

	"github.com/syndtr/goleveldb/leveldb"
)

type ldb struct {
	db *leveldb.DB
}

// NewLevelDB returns a levelDB implementation of the Store.
func NewLevelDB(db *leveldb.DB) Store {
	return &ldb{
		db: db,
	}
}

// Read implements the `Store` interface.
func (db *ldb) Read(key string, value interface{}) error {
	data, err := db.db.Get([]byte(key), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			err = ErrKeyNotFound
		}
		return err
	}
	return json.Unmarshal(data, value)
}

// ReadData implements the `Store` interface.
func (db *ldb) ReadData(key string) ([]byte, error) {
	value, err := db.db.Get([]byte(key), nil)
	if err == leveldb.ErrNotFound {
		err = ErrKeyNotFound
	}

	return value, err
}

// Write implements the `Store` interface.
func (db *ldb) Write(key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return db.db.Put([]byte(key), data, nil)
}

// WriteData implements the `Store` interface.
func (db *ldb) WriteData(key string, data []byte) error {
	return db.db.Put([]byte(key), data, nil)
}

// Delete implements the `Store` interface.
func (db *ldb) Delete(key string) error {
	return db.db.Delete([]byte(key), nil)
}

package store

import (
	"encoding/json"
	"fmt"

	"github.com/dgraph-io/badger"
	"github.com/syndtr/goleveldb/leveldb"
)

var ErrKeyNotFound = fmt.Errorf("key not found")

type Store interface {
	Read(key string, value interface{}) error
	Write(key string, value interface{}) error
	ReadData(key string) ([]byte, error)
	WriteData(key string, data []byte) error
	Delete(key string) error
}

type cache map[string][]byte

func NewCache() Store {
	return cache{}
}

func (cache cache) Read(key string, value interface{}) error {
	val, ok := cache[key]
	if !ok {
		return ErrKeyNotFound
	}
	return json.Unmarshal(val, value)
}

func (cache cache) ReadData(key string) ([]byte, error) {
	val, ok := cache[key]
	if !ok {
		return nil, ErrKeyNotFound
	}
	return val, nil
}

func (cache cache) Write(key string, value interface{}) error {
	val, err := json.Marshal(value)
	if err != nil {
		return err
	}
	cache[key] = val
	return nil
}

func (cache cache) WriteData(key string, data []byte) error {
	cache[key] = data
	return nil
}

func (cache cache) Delete(key string) error {
	delete(cache, key)
	return nil
}

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

type bdb struct {
	db *badger.DB
}

func NewBadgerDB(db *badger.DB) Store {
	return &bdb{
		db: db,
	}
}

func (db *bdb) Read(key string, value interface{}) error {
	return db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		data, err := item.Value()
		if err != nil {
			return err
		}
		return json.Unmarshal(data, value)
	})
}

func (db *bdb) ReadData(key string) (data []byte, err error) {
	err = db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		data, err = item.Value()
		return err
	})
	return
}

func (db *bdb) Write(key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return db.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), data)
	})
}

func (db *bdb) WriteData(key string, data []byte) error {
	return db.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), data)
	})
}

func (db *bdb) Delete(key string) error {
	return db.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}

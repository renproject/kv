package store

import (
	"encoding/json"

	"github.com/dgraph-io/badger"
)

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

package store

import (
	"encoding/json"

	"github.com/dgraph-io/badger"
)

type bdb struct {
	db *badger.DB
}

// NewBadgerDB returns a badgerDB implementation of the Store.
func NewBadgerDB(db *badger.DB) IterableStore {
	return &bdb{
		db: db,
	}
}

// Read implements the `Store` interface.
func (db *bdb) Read(key string, value interface{}) error {
	err := db.db.View(func(txn *badger.Txn) error {
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
	if err == badger.ErrKeyNotFound {
		return ErrKeyNotFound
	}
	return err
}

// ReadData implements the `Store` interface.
func (db *bdb) ReadData(key string) (data []byte, err error) {
	err = db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		data, err = item.Value()
		return err
	})
	if err == badger.ErrKeyNotFound {
		err = ErrKeyNotFound
	}
	return
}

// Write implements the `Store` interface.
func (db *bdb) Write(key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return db.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), data)
	})
}

// WriteData implements the `Store` interface.
func (db *bdb) WriteData(key string, data []byte) error {
	return db.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), data)
	})
}

// Delete implements the `Store` interface.
func (db *bdb) Delete(key string) error {
	return db.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}

func (db *bdb) Entries() (int, error) {
	count := 0
	err := db.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})

	return count, err
}

func (db *bdb) Iterator() Iterator {
	tx := db.db.NewTransaction(false)
	iter := tx.NewIterator(badger.DefaultIteratorOptions)
	iter.Rewind()
	return &BadgerIterator{
		isFirst: true,
		tx:      tx,
		iter:    iter,
	}
}

type BadgerIterator struct {
	isFirst bool
	tx      *badger.Txn
	iter    *badger.Iterator
}

func (iter *BadgerIterator) Next() bool {
	if iter.isFirst {
		iter.isFirst = false
	} else {
		iter.iter.Next()
	}
	valid := iter.iter.Valid()
	if !valid {
		iter.iter.Close()
		iter.tx.Discard()
	}

	return valid
}

func (iter *BadgerIterator) Key() (string, error) {
	return string(iter.iter.Item().Key()), nil
}

func (iter *BadgerIterator) Value(value interface{}) error {
	data, err := iter.iter.Item().Value()
	if err != nil {
		return err
	}

	return json.Unmarshal(data, value)
}

package badgerdb

import (
	"github.com/dgraph-io/badger"
	"github.com/renproject/kv/db"
)

type bdb struct {
	db *badger.DB
}

func New(db *badger.DB) db.Iterable {
	return &bdb{
		db: db,
	}
}

func (bdb *bdb) Insert(key string, value []byte) error {
	return bdb.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), value)
	})
}

func (bdb *bdb) Get(key string) (value []byte, err error) {
	err = bdb.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		return item.Value(func(data []byte) error {
			value = data
			return nil
		})
	})
	if err == badger.ErrKeyNotFound {
		err = db.ErrNotFound
	}
	return
}

func (db *bdb) Delete(key string) error {
	return db.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}

func (db *bdb) Size() (int, error) {
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

func (db *bdb) Iterator() db.Iterator {
	tx := db.db.NewTransaction(false)
	iter := tx.NewIterator(badger.DefaultIteratorOptions)
	iter.Rewind()
	return &BadgerIterator{
		isFirst: true,
		tx:      tx,
		iter:    iter,
	}
}

// BadgerIterator implements the `IterableStore` interface.
type BadgerIterator struct {
	isFirst bool
	tx      *badger.Txn
	iter    *badger.Iterator
}

// Next implements the `Iterator` interface.
func (iter *BadgerIterator) Next() bool {
	if iter.isFirst {
		iter.isFirst = false
	} else {
		iter.iter.Next()
	}
	if valid := iter.iter.Valid(); !valid {
		iter.iter.Close()
		iter.tx.Discard()
		return false
	}
	return true
}

// Key implements the `Iterator` interface.
func (iter *BadgerIterator) Key() (string, error) {
	return string(iter.iter.Item().Key()), nil
}

// Value implements the `Iterator` interface.
func (iter *BadgerIterator) Value() (value []byte, err error) {
	err = iter.iter.Item().Value(func(data []byte) error {
		value = data
		return nil
	})
	return
}

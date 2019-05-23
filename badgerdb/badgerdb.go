package badgerdb

import (
	"github.com/dgraph-io/badger"
	"github.com/renproject/kv/db"
)

// bdb is a badgerDB implementation of the `db.Iterable`.
type bdb struct {
	db *badger.DB
}

// New returns a new `db.Iterable`.
func New(db *badger.DB) db.Iterable {
	return &bdb{
		db: db,
	}
}

// Insert implements the `db.Iterable` interface
func (bdb *bdb) Insert(key string, value []byte) error {
	return bdb.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), value)
	})
}

// Get implements the `db.Iterable` interface
func (bdb *bdb) Get(key string) (value []byte, err error) {
	err = bdb.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		value, err = item.Value()
		return err
	})
	if err == badger.ErrKeyNotFound {
		err = db.ErrNotFound
	}
	return
}

// Delete implements the `db.Iterable` interface
func (db *bdb) Delete(key string) error {
	return db.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}

// Size implements the `db.Iterable` interface
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

// Iterator implements the `db.Iterable` interface
func (db *bdb) Iterator() db.Iterator {
	tx := db.db.NewTransaction(false)
	iter := tx.NewIterator(badger.DefaultIteratorOptions)
	iter.Rewind()
	return &Iterator{
		isFirst:  true,
		isClosed: false,
		tx:       tx,
		iter:     iter,
	}
}

// Iterator implements the `db.Iterator` interface.
type Iterator struct {
	isFirst  bool
	isClosed bool
	tx       *badger.Txn
	iter     *badger.Iterator
}

// Next implements the `db.Iterator` interface.
func (iter *Iterator) Next() bool {
	if iter.isClosed {
		return false
	}
	if iter.isFirst {
		iter.isFirst = false
	} else {
		iter.iter.Next()
	}
	if valid := iter.iter.Valid(); !valid {
		iter.isClosed = true
		iter.iter.Close()
		iter.tx.Discard()
		return false
	}
	return true
}

// Key implements the `db.Iterator` interface.
func (iter *Iterator) Key() (string, error) {
	if iter.isClosed || !iter.iter.Valid() {
		return "", db.ErrIndexOutOfRange
	}
	return string(iter.iter.Item().Key()), nil
}

// Value implements the `db.Iterator` interface.
func (iter *Iterator) Value() ([]byte, error) {
	if iter.isClosed || !iter.iter.Valid() {
		return nil, db.ErrIndexOutOfRange
	}
	return iter.iter.Item().Value()
}

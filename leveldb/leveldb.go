package leveldb

import (
	"github.com/renproject/kv/db"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

// ldb is levelDB implementation of the `db.Iterable`
type ldb struct {
	db *leveldb.DB
}

// New returns a new ldb.
func New(db *leveldb.DB) db.Iterable {
	return &ldb{
		db: db,
	}
}

// Insert implements the `db.Iterable` interface.
func (ldb *ldb) Insert(key string, data []byte) error {
	if key == "" {
		return db.ErrEmptyKey
	}
	return ldb.db.Put([]byte(key), data, nil)
}

// Get implements the `db.Iterable` interface.
func (ldb *ldb) Get(key string) (value []byte, err error) {
	value, err = ldb.db.Get([]byte(key), nil)
	if err == leveldb.ErrNotFound {
		err = db.ErrNotFound
	}
	return
}

// Delete implements the `db.Iterable` interface.
func (ldb *ldb) Delete(key string) error {
	return ldb.db.Delete([]byte(key), nil)
}

// Size implements the `db.Iterable` interface.
func (ldb ldb) Size() (int, error) {
	iter := ldb.db.NewIterator(nil, nil)
	count := 0
	for iter.Next() {
		count++
	}
	iter.Release()
	return count, iter.Error()
}

// Iterator implements the `db.Iterable` interface.
func (ldb ldb) Iterator() db.Iterator {
	iter := ldb.db.NewIterator(nil, nil)
	return &Iterator{
		inRange: false,
		iter:    iter,
	}
}

// Iterator implements the `db.Iterator` with leveldb iterator.
type Iterator struct {
	inRange bool
	iter    iterator.Iterator
}

// Next implements the `db.Iterator`.
func (iter *Iterator) Next() bool {
	iter.inRange = true
	next := iter.iter.Next()
	if !next {
		iter.inRange = false
	}
	return next
}

// Key implements the `db.Iterator`.
func (iter *Iterator) Key() (string, error) {
	if !iter.inRange {
		return "", db.ErrIndexOutOfRange
	}
	return string(iter.iter.Key()), nil
}

// Value implements the `db.Iterator`.
func (iter *Iterator) Value() ([]byte, error) {
	if !iter.inRange {
		return nil, db.ErrIndexOutOfRange
	}
	return iter.iter.Value(), nil
}
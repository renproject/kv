package leveldb

import (
	"bytes"
	"fmt"

	"github.com/renproject/kv/db"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// levelDB is a leveldb implementation of the `db.Iterable`.
type levelDB struct {
	db    *leveldb.DB
	codec db.Codec
}

// New returns a new `db.Iterable`.
func New(path string, codec db.Codec) db.DB {
	if codec == nil {
		panic("codec cannot be nil")
	}

	ldb, err := leveldb.OpenFile(path, nil)
	if err != nil {
		panic(fmt.Sprintf("error initialising leveldb: %v", err))
	}

	return &levelDB{
		db:    ldb,
		codec: codec,
	}
}

func (ldb *levelDB) Close() error {
	return ldb.db.Close()
}

// Insert implements the `db.DB` interface.
func (ldb *levelDB) Insert(key string, value interface{}) error {
	if key == "" {
		return db.ErrEmptyKey
	}
	data, err := ldb.codec.Encode(value)
	if err != nil {
		return err
	}

	return ldb.db.Put([]byte(key), data, nil)
}

// Get implements the `db.DB` interface.
func (ldb *levelDB) Get(key string, value interface{}) error {
	if key == "" {
		return db.ErrEmptyKey
	}

	data, err := ldb.db.Get([]byte(key), nil)
	if err != nil {
		return convertErr(err)
	}

	return ldb.codec.Decode(data, value)
}

// Delete implements the `db.DB` interface.
func (ldb *levelDB) Delete(key string) error {
	if key == "" {
		return db.ErrEmptyKey
	}
	return ldb.db.Delete([]byte(key), nil)
}

// Size implements the `db.DB` interface.
func (ldb *levelDB) Size(prefix string) (int, error) {
	iter := ldb.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	defer iter.Release()

	counter := 0
	for iter.Next() {
		counter++
	}
	return counter, nil
}

// Iterator implements the `db.DB` interface.
func (ldb *levelDB) Iterator(prefix string) db.Iterator {
	iterator := ldb.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	return &iter{
		prefix: []byte(prefix),
		iter:   iterator,
		codec:  ldb.codec,
	}
}

// iter implements the `db.Iterator` interface.
type iter struct {
	prefix []byte
	iter   iterator.Iterator
	codec  db.Codec
}

// Next implements the `db.Iterator` interface.
func (iter *iter) Next() bool {
	next := iter.iter.Next()

	// Release the iter when it finishes iterating.
	if !next {
		iter.iter.Release()
	}
	return next
}

// Key implements the `db.Iterator` interface.
func (iter *iter) Key() (string, error) {
	key := iter.iter.Key()
	if key == nil {
		return "", db.ErrIndexOutOfRange
	}
	return string(bytes.TrimPrefix(key, iter.prefix)), nil
}

// Value implements the `db.Iterator` interface.
func (iter *iter) Value(value interface{}) error {
	val := iter.iter.Value()
	if val == nil {
		return db.ErrIndexOutOfRange
	}
	return iter.codec.Decode(val, value)
}

// Close implements the `db.Iterator` interface.
func (iter *iter) Close() {
	iter.iter.Release()
}

// convertErr will convert levelDB-specific error to kv error.
func convertErr(err error) error {
	switch err {
	case leveldb.ErrNotFound:
		return db.ErrKeyNotFound
	default:
		return err
	}
}

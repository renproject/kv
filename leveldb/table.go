package leveldb

import (
	"bytes"
	"fmt"

	"github.com/renproject/kv/db"
	"github.com/syndtr/goleveldb/leveldb"
	levelIter "github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
	"golang.org/x/crypto/sha3"
)

// KeyPrefix add hash of the table name to the key so that data entries are
// categorised into different tables.
func KeyPrefix(hash [32]byte, key []byte) []byte {
	if key == nil {
		return hash[:]
	}
	return append(hash[:], key...)
}

// table is a leveldb implementation of the `db.table`.
type table struct {
	hash  [32]byte
	db    *leveldb.DB
	codec db.Codec
}

// NewTable returns a new levelDB implementation of the `db.table`.
func NewTable(name string, ldb *leveldb.DB, codec db.Codec) db.Table {
	return &table{
		hash:  sha3.Sum256([]byte(name)),
		db:    ldb,
		codec: codec,
	}
}

// Insert implements the `db.table` interface.
func (t *table) Insert(key string, value interface{}) error {
	if key == "" {
		return db.ErrEmptyKey
	}
	data, err := t.codec.Encode(value)
	if err != nil {
		return err
	}

	return t.db.Put(KeyPrefix(t.hash, []byte(key)), data, nil)
}

// Get implements the `db.table` interface.
func (t *table) Get(key string, value interface{}) error {
	if key == "" {
		return db.ErrEmptyKey
	}

	val, err := t.db.Get(KeyPrefix(t.hash, []byte(key)), nil)
	if err != nil {
		return convertErr(err)
	}
	return t.codec.Decode(val, value)
}

// Delete implements the `db.table` interface.
func (t *table) Delete(key string) error {
	return t.db.Delete(KeyPrefix(t.hash, []byte(key)), nil)
}

// Size implements the `db.table` interface.
func (t *table) Size() (int, error) {
	count := 0
	iter := t.db.NewIterator(util.BytesPrefix(t.hash[:]), nil)
	for iter.Next() {
		count++
	}
	iter.Release()
	return count, nil
}

// Iterator implements the `db.table` interface.
func (t *table) Iterator() (db.Iterator, error) {
	iter := t.db.NewIterator(util.BytesPrefix(t.hash[:]), nil)
	return &iterator{
		hash:  t.hash,
		iter:  iter,
		codec: t.codec,
	}, nil
}

// iterator implements the `db.Iterator` interface.
type iterator struct {
	hash  [32]byte
	iter  levelIter.Iterator
	codec db.Codec
}

// Next implements the `db.Iterator` interface.
func (iter *iterator) Next() bool {
	next := iter.iter.Next()

	// Release the iterator when it finishes iterating.
	if !next {
		iter.iter.Release()
	}
	return next
}

// Key implements the `db.Iterator` interface.
func (iter *iterator) Key() (string, error) {
	key := iter.iter.Key()
	if key == nil {
		return "", db.ErrIndexOutOfRange
	}
	if !bytes.HasPrefix(key, KeyPrefix(iter.hash, nil)) {
		return "", fmt.Errorf("invalid key = %x which doesn't have valid prefix", key)
	}
	return string(bytes.TrimPrefix(key, KeyPrefix(iter.hash, nil))), nil
}

// Value implements the `db.Iterator` interface.
func (iter *iterator) Value(value interface{}) error {
	val := iter.iter.Value()
	if val == nil {
		return db.ErrIndexOutOfRange
	}
	return iter.codec.Decode(val, value)
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

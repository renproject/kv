package badgerdb

import (
	"bytes"
	"fmt"

	"github.com/dgraph-io/badger"
	"github.com/renproject/kv/db"
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

// table is a badgerDB implementation of the `db.table`.
type table struct {
	hash  [32]byte
	db    *badger.DB
	codec db.Codec
}

// NewTable returns a new badgerDB implementation of the `db.table`.
func NewTable(name string, bdb *badger.DB, codec db.Codec) db.Table {
	if codec == nil {
		panic("codec cannot be nil")
	}
	return &table{
		hash:  sha3.Sum256([]byte(name)),
		db:    bdb,
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

	err = t.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(KeyPrefix(t.hash, []byte(key))), data)
	})
	return convertErr(err)
}

// Get implements the `db.table` interface.
func (t *table) Get(key string, value interface{}) error {
	if key == "" {
		return db.ErrEmptyKey
	}

	err := t.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(KeyPrefix(t.hash, []byte(key))))
		if err != nil {
			return err
		}

		data, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return t.codec.Decode(data, value)
	})
	return convertErr(err)
}

// Delete implements the `db.table` interface.
func (t *table) Delete(key string) error {
	if key == "" {
		return db.ErrEmptyKey
	}

	err := t.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(KeyPrefix(t.hash, []byte(key))))
	})

	return convertErr(err)
}

// Size implements the `db.table` interface.
func (t *table) Size() (int, error) {
	count := 0
	err := t.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = KeyPrefix(t.hash, []byte{})
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})

	return count, err
}

// Iterator implements the `db.table` interface.
func (t *table) Iterator() (db.Iterator, error) {
	tx := t.db.NewTransaction(false)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = KeyPrefix(t.hash, nil)
	iter := tx.NewIterator(opts)
	iter.Rewind()
	return &iterator{
		hash:       t.hash,
		intialized: false,
		tx:         tx,
		iter:       iter,
		codec:      t.codec,
	}, nil
}

// iterator implements the `db.Iterator` interface.
type iterator struct {
	hash       [32]byte
	intialized bool
	tx         *badger.Txn
	iter       *badger.Iterator
	codec      db.Codec
}

// Next implements the `db.Iterator` interface.
func (iter *iterator) Next() bool {
	if !iter.intialized {
		iter.intialized = true
	} else {
		if !iter.iter.Valid() {
			return false
		}
		iter.iter.Next()
	}

	if valid := iter.iter.Valid(); !valid {
		iter.iter.Close()
		iter.tx.Discard()
		return false
	}
	return true
}

// Key implements the `db.Iterator` interface.
func (iter *iterator) Key() (string, error) {
	if !iter.intialized || !iter.iter.Valid() {
		return "", db.ErrIndexOutOfRange
	}
	key := iter.iter.Item().Key()
	if !bytes.HasPrefix(key, KeyPrefix(iter.hash, nil)) {
		return "", fmt.Errorf("invalid key = %x which doesn't have valid prefix", key)
	}
	return string(bytes.TrimPrefix(key, KeyPrefix(iter.hash, nil))), nil
}

// Value implements the `db.Iterator` interface.
func (iter *iterator) Value(value interface{}) error {
	if !iter.intialized || !iter.iter.Valid() {
		return db.ErrIndexOutOfRange
	}
	data, err := iter.iter.Item().ValueCopy(nil)
	if err != nil {
		return err
	}
	return iter.codec.Decode(data, value)
}

// convertErr will convert badgerDB-specific error to kv error.
func convertErr(err error) error {
	switch err {
	case badger.ErrEmptyKey:
		return db.ErrEmptyKey
	case badger.ErrKeyNotFound:
		return db.ErrKeyNotFound
	default:
		return err
	}
}

package badgerdb

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
	"github.com/renproject/kv/db"
	"golang.org/x/crypto/sha3"
)

// badgerDB is a badgerDB implementation of the `db.Iterable`.
type badgerDB struct {
	mu       *sync.Mutex
	prefixes map[string][]byte
	db     *badger.DB
	codec    db.Codec
}

// New returns a new `db.Iterable`.
func New(path string , codec db.Codec) db.DB {
	if codec == nil {
		panic("codec cannot be nil")
	}

	opts := badger.DefaultOptions("./.badgerdb")
	db, err := badger.Open(opts)
	if err != nil {
		panic(fmt.Sprintf("fail to initialize badgerDB, err = %v", err))
	}

	bdb:= &badgerDB{
		mu:       new(sync.Mutex),
		prefixes: map[string][]byte{},
		db:       db,
		codec:    codec,
	}

	go bdb.gc()

	return bdb
}

// Close implements the `db.DB` interface.
func (bdb *badgerDB) Close() error {
	return bdb.db.Close()
}

// Insert implements the `db.DB` interface.
func (bdb *badgerDB) Insert(name string, key string, value interface{}) error {
	if key == "" {
		return db.ErrEmptyKey
	}
	keyBytes := append(bdb.prefix(name), []byte(key)...)
	data, err := bdb.codec.Encode(value)
	if err != nil {
		return err
	}

	err = bdb.db.Update(func(txn *badger.Txn) error {
		return txn.Set(keyBytes, data)
	})
	return convertErr(err)
}

// Get implements the `db.DB` interface.
func (bdb *badgerDB) Get(name string, key string, value interface{}) error {
	if key == "" {
		return db.ErrEmptyKey
	}
	keyBytes := append(bdb.prefix(name), []byte(key)...)

	err := bdb.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(keyBytes)
		if err != nil {
			return err
		}

		data, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return bdb.codec.Decode(data, value)
	})
	return convertErr(err)
}

// Delete implements the `db.DB` interface.
func (bdb *badgerDB) Delete(name string, key string) error {
	if key == "" {
		return db.ErrEmptyKey
	}
	keyBytes := append(bdb.prefix(name), []byte(key)...)

	err := bdb.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(keyBytes)
	})
	return convertErr(err)
}

// Size implements the `db.DB` interface.
func (bdb *badgerDB) Size(name string) (int, error) {
	count := 0
	err := bdb.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = bdb.prefix(name)
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})
	return count, err
}

// Iterator implements the `db.DB` interface.
func (bdb *badgerDB) Iterator(name string) db.Iterator {
	tx := bdb.db.NewTransaction(false)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = bdb.prefix(name)
	iter := tx.NewIterator(opts)
	iter.Rewind()
	return &iterator{
		prefix:     opts.Prefix,
		intialized: false,
		tx:         tx,
		iter:       iter,
		codec:      bdb.codec,
	}
}

func (bdb *badgerDB) prefix(name string) []byte {
	bdb.mu.Lock()
	defer bdb.mu.Unlock()

	if prefix, ok := bdb.prefixes[name]; ok {
		return prefix
	}
	prefix := sha3.Sum256([]byte(name))
	bdb.prefixes[name] = prefix[:]
	return prefix[:]
}

func (bdb *badgerDB) gc() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		err := bdb.db.RunValueLogGC(0.5)
		if err != nil {
			return
		}
	}
}

// iterator implements the `db.Iterator` interface.
type iterator struct {
	prefix     []byte
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
	if !bytes.HasPrefix(key, iter.prefix) {
		return "", fmt.Errorf("invalid key = %x which doesn't have valid prefix", key)
	}
	return string(bytes.TrimPrefix(key, iter.prefix)), nil
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

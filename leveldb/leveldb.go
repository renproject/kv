package leveldb

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/renproject/kv/db"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
	"golang.org/x/crypto/sha3"
)

// levelDB is a leveldb implementation of the `db.Iterable`.
type levelDB struct {
	mu       *sync.Mutex
	prefixes map[string][]byte
	db       *leveldb.DB
	codec    db.Codec
}

// New returns a new `db.Iterable`.
func New(path string, codec db.Codec) db.DB {
	if codec == nil {
		panic("codec cannot be nil")
	}

	ldb, err := leveldb.OpenFile(path, nil)
	if err != nil {
		panic(fmt.Sprintf("fail to initialize leveldb, err = %v", err))
	}

	return &levelDB{
		mu:       new(sync.Mutex),
		prefixes: map[string][]byte{},
		db:       ldb,
		codec:    codec,
	}
}

func (ldb *levelDB) Close() error {
	return ldb.db.Close()
}

// Insert implements the `db.DB` interface.
func (ldb *levelDB) Insert(name string, key string, value interface{}) error {
	if key == "" {
		return db.ErrEmptyKey
	}
	keyBytes := append(ldb.prefix(name), []byte(key)...)
	data, err := ldb.codec.Encode(value)
	if err != nil {
		return err
	}

	return ldb.db.Put(keyBytes, data, nil)
}

// Get implements the `db.DB` interface.
func (ldb *levelDB) Get(name string, key string, value interface{}) error {
	if key == "" {
		return db.ErrEmptyKey
	}

	keyBytes := append(ldb.prefix(name), []byte(key)...)
	data, err := ldb.db.Get(keyBytes, nil)
	if err != nil {
		return convertErr(err)
	}

	return ldb.codec.Decode(data, value)
}

// Delete implements the `db.DB` interface.
func (ldb *levelDB) Delete(name string, key string) error {
	if key == "" {
		return db.ErrEmptyKey
	}

	keyBytes := append(ldb.prefix(name), []byte(key)...)
	return ldb.db.Delete(keyBytes, nil)
}

// Size implements the `db.DB` interface.
func (ldb *levelDB) Size(name string) (int, error) {
	prefix := ldb.prefix(name)
	iter := ldb.db.NewIterator(util.BytesPrefix(prefix), nil)
	counter := 0
	for iter.Next() {
		counter++
	}
	iter.Release()
	return counter, nil
}

// Iterator implements the `db.DB` interface.
func (ldb *levelDB) Iterator(name string) db.Iterator {
	prefix := ldb.prefix(name)
	iterator := ldb.db.NewIterator(util.BytesPrefix(prefix), nil)
	return &iter{
		prefix: prefix,
		iter:   iterator,
		codec:  ldb.codec,
	}
}

func (ldb *levelDB) prefix(name string) []byte {
	ldb.mu.Lock()
	defer ldb.mu.Unlock()

	if prefix, ok := ldb.prefixes[name]; ok {
		return prefix
	}
	prefix := sha3.Sum256([]byte(name))
	ldb.prefixes[name] = prefix[:]
	return prefix[:]
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
	if !bytes.HasPrefix(key, iter.prefix) {
		return "", fmt.Errorf("invalid key = %x which doesn't have valid prefix", key)
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

// convertErr will convert levelDB-specific error to kv error.
func convertErr(err error) error {
	switch err {
	case leveldb.ErrNotFound:
		return db.ErrKeyNotFound
	default:
		return err
	}
}

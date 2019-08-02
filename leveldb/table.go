package leveldb

import (
	"github.com/renproject/kv/db"
	"github.com/syndtr/goleveldb/leveldb"
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

// table is a levelDB implementation of the `db.Table`.
type table struct {
	hash  [32]byte
	db    *leveldb.DB
	codec db.Codec
}

// NewTable returns a new levelDB implementation of the `db.Table`.
func NewTable(name string, ldb *leveldb.DB, codec db.Codec) db.Table {
	return &table{
		hash:  sha3.Sum256([]byte(name)),
		db:    ldb,
		codec: codec,
	}
}

// Insert implements the `db.Table` interface.
func (t *table) Insert(key string, value interface{}) error {
	if key == "" {
		return db.ErrEmptyKey
	}
	data, err := t.codec.Encode(value)
	if err != nil {
		return err
	}

	return convertErr(t.db.Put([]byte(KeyPrefix(t.hash, []byte(key))), data, nil))
}

// Get implements the `db.Table` interface.
func (t *table) Get(key string, value interface{}) error {
	if key == "" {
		return db.ErrEmptyKey
	}

	val, err := t.db.Get([]byte(KeyPrefix(t.hash, []byte(key))), nil)
	if err != nil {
		return convertErr(err)
	}
	return convertErr(t.codec.Decode(val, value))
}

// Delete implements the `db.Table` interface.
func (t *table) Delete(key string) error {
	return convertErr(t.db.Delete([]byte(KeyPrefix(t.hash, []byte(key))), nil))
}

// Size implements the `db.Table` interface.
func (t *table) Size() (int, error) {
	count := 0
	// t.db.NewIterator(nil, &opt.ReadOptions{})
	// err := t.db.
	// 	opts := badger.DefaultIteratorOptions
	// 	opts.Prefix = KeyPrefix(t.hash, []byte{})
	// 	it := txn.NewIterator(opts)
	// 	defer it.Close()
	// 	for it.Rewind(); it.Valid(); it.Next() {
	// 		count++
	// 	}
	// 	return nil
	// })

	return count, nil
}

// Iterator implements the `db.Table` interface.
func (t *table) Iterator() (db.Iterator, error) {
	// tx := t.db.NewTransaction(false)
	// opts := badger.DefaultIteratorOptions
	// opts.Prefix = KeyPrefix(t.hash, nil)
	// iter := tx.NewIterator(opts)
	// iter.Rewind()
	return &iterator{
		hash:       t.hash,
		intialized: false,
		closed:     false,
		tx:         nil,
		// iter:       nil,
		codec: t.codec,
	}, nil
}

// iterator implements the `db.Iterator` interface.
type iterator struct {
	hash       [32]byte
	intialized bool
	closed     bool
	tx         *leveldb.Transaction
	// iter       *leveldb.
	codec db.Codec
}

// Next implements the `db.Iterator` interface.
func (iter *iterator) Next() bool {
	if iter.closed {
		return false
	}
	if !iter.intialized {
		iter.intialized = true
	} else {
		// iter.iter.Next()
	}
	// if valid := iter.iter.Valid(); !valid {
	// 	iter.closed = true
	// 	iter.iter.Close()
	// 	iter.tx.Discard()
	// 	return false
	// }
	return true
}

// Key implements the `db.Iterator` interface.
func (iter *iterator) Key() (string, error) {
	// if !iter.intialized || iter.closed || !iter.iter.Valid() {
	// 	return "", db.ErrIndexOutOfRange
	// }
	// key := iter.iter.Item().Key()
	// if !bytes.HasPrefix(key, KeyPrefix(iter.hash, nil)) {
	// 	return "", fmt.Errorf("invalid key = %x which doesn't have valid prefix", key)
	// }
	// return string(bytes.TrimPrefix(key, KeyPrefix(iter.hash, nil))), nil
	return "", nil
}

// Value implements the `db.Iterator` interface.
func (iter *iterator) Value(value interface{}) error {
	// if !iter.intialized || iter.closed || !iter.iter.Valid() {
	// 	return db.ErrIndexOutOfRange
	// }
	// data, err := iter.iter.Item().ValueCopy(nil)
	// if err != nil {
	// 	return err
	// }
	// return iter.codec.Decode(data, value)
	return nil
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

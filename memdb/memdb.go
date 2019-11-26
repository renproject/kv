package memdb

import (
	"strings"
	"sync"

	"github.com/renproject/kv/db"
)

// memdb is a in-memory implementation of the `db.DB`.
type memdb struct {
	prefixMu *sync.Mutex
	prefixes map[string]string

	dataMu *sync.RWMutex
	data   map[string][]byte
	codec  db.Codec
}

// New returns a new memdb.
func New(codec db.Codec) db.DB {
	if codec == nil {
		panic("codec cannot be nil")
	}
	return &memdb{
		prefixMu: new(sync.Mutex),
		prefixes: map[string]string{},
		dataMu:   new(sync.RWMutex),
		data:     map[string][]byte{},
		codec:    codec,
	}
}

// Close implements the `db.DB` interface.
func (memdb *memdb) Close() error {
	return nil
}

// Insert implements the `db.DB` interface.
func (memdb *memdb) Insert(key string, value interface{}) error {
	if key == "" {
		return db.ErrEmptyKey
	}

	memdb.dataMu.Lock()
	defer memdb.dataMu.Unlock()

	data, err := memdb.codec.Encode(value)
	if err != nil {
		return err
	}

	memdb.data[key] = data

	return nil
}

// Get implements the `db.DB` interface.
func (memdb *memdb) Get(key string, value interface{}) error {
	if key == "" {
		return db.ErrEmptyKey
	}

	memdb.dataMu.RLock()
	defer memdb.dataMu.RUnlock()

	data, ok := memdb.data[key]
	if !ok {
		return db.ErrKeyNotFound
	}
	return memdb.codec.Decode(data, value)
}

// Delete implements the `db.DB` interface.
func (memdb *memdb) Delete(key string) error {
	if key == "" {
		return db.ErrEmptyKey
	}

	memdb.dataMu.Lock()
	defer memdb.dataMu.Unlock()

	delete(memdb.data, key)
	return nil
}

// Size implements the `db.DB` interface.
func (memdb *memdb) Size(prefix string) (int, error) {
	memdb.dataMu.RLock()
	defer memdb.dataMu.RUnlock()

	counter := 0
	for key := range memdb.data {
		if strings.HasPrefix(key, prefix) {
			counter++
		}
	}
	return counter, nil
}

// Iterator implements the `db.DB` interface.
func (memdb *memdb) Iterator(prefix string) db.Iterator {
	memdb.dataMu.RLock()
	defer memdb.dataMu.RUnlock()

	iter := &iterator{
		index:  -1,
		codec:  memdb.codec,
		keys:   make([]string, 0, len(memdb.data)),
		values: make([][]byte, 0, len(memdb.data)),
	}
	for key, value := range memdb.data {
		if strings.HasPrefix(key, prefix) {
			iter.keys = append(iter.keys, strings.TrimPrefix(key, prefix))
			iter.values = append(iter.values, value)
		}
	}

	return iter
}

// iterator is a in-memory implementation of the `db.Iterator`.
type iterator struct {
	index int
	codec db.Codec

	keys   []string
	values [][]byte
}

// Next implements the `db.Iterator` interface.
func (iter *iterator) Next() bool {
	iter.index++
	return iter.index < len(iter.keys)
}

// Key implements the `db.Iterator` interface.
func (iter *iterator) Key() (string, error) {
	if iter.index == -1 || iter.index >= len(iter.keys) {
		return "", db.ErrIndexOutOfRange
	}

	return iter.keys[iter.index], nil
}

// Value implements the `db.Iterator` interface.
func (iter *iterator) Value(value interface{}) error {
	if iter.index == -1 || iter.index >= len(iter.keys) {
		return db.ErrIndexOutOfRange
	}
	data := iter.values[iter.index]
	return iter.codec.Decode(data, value)
}

// Close implements the `db.Iterator` interface.
func (iter *iterator) Close() {}

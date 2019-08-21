package memdb

import (
	"fmt"
	"strings"
	"sync"

	"github.com/renproject/kv/db"
	"golang.org/x/crypto/sha3"
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
func (memdb *memdb) Insert(name string, key string, value interface{}) error {
	if key == "" {
		return db.ErrEmptyKey
	}
	prefix := memdb.prefix(name)

	memdb.dataMu.Lock()
	defer memdb.dataMu.Unlock()

	data, err := memdb.codec.Encode(value)
	if err != nil {
		return err
	}

	memdb.data[memdb.prefixKey(prefix, key)] = data

	return nil
}

// Get implements the `db.DB` interface.
func (memdb *memdb) Get(name string, key string, value interface{}) error {
	if key == "" {
		return db.ErrEmptyKey
	}
	prefix := memdb.prefix(name)

	memdb.dataMu.RLock()
	defer memdb.dataMu.RUnlock()

	data, ok := memdb.data[memdb.prefixKey(prefix, key)]
	if !ok {
		return db.ErrKeyNotFound
	}
	return memdb.codec.Decode(data, value)
}

// Delete implements the `db.DB` interface.
func (memdb *memdb) Delete(name string, key string) error {
	if key == "" {
		return db.ErrEmptyKey
	}
	prefix := memdb.prefix(name)

	memdb.dataMu.Lock()
	defer memdb.dataMu.Unlock()

	delete(memdb.data, memdb.prefixKey(prefix, key))
	return nil
}

// Size implements the `db.DB` interface.
func (memdb *memdb) Size(name string) (int, error) {
	prefix := memdb.prefix(name)

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
func (memdb *memdb) Iterator(name string) db.Iterator {
	prefix := memdb.prefix(name)

	tableData := map[string][]byte{}
	memdb.dataMu.RLock()
	defer memdb.dataMu.RUnlock()

	for key, value := range memdb.data {
		if strings.HasPrefix(key, prefix) {
			tableData[key] = value
		}
	}

	return newIterator(name, tableData, memdb.codec)
}

func (memdb *memdb) prefix(name string) string {
	memdb.prefixMu.Lock()
	defer memdb.prefixMu.Unlock()

	if prefix, ok := memdb.prefixes[name]; ok {
		return prefix
	}
	hash := sha3.Sum256([]byte(name))
	prefix := string(hash[:])
	memdb.prefixes[name] = prefix
	return prefix
}

func (memdb *memdb) prefixKey(prefix, key string) string {
	return fmt.Sprintf("%v%v", prefix, key)
}

// iterator is a in-memory implementation of the `db.Iterator`.
type iterator struct {
	index  int
	keys   []string
	values [][]byte
	codec  db.Codec
}

// newIterator returns a `db.Iterator` with a
func newIterator(name string, data map[string][]byte, codec db.Codec) db.Iterator {
	keys := make([]string, 0, len(data))
	values := make([][]byte, 0, len(data))
	for key, value := range data {
		hash := sha3.Sum256([]byte(name))
		keys = append(keys, strings.TrimPrefix(key, string(hash[:])))
		values = append(values, value)
	}

	return &iterator{
		index:  -1,
		keys:   keys,
		values: values,
		codec:  codec,
	}
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

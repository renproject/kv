package memdb

import (
	"sync"

	"github.com/renproject/kv/db"
)

// table is a in-memory implementation of the `db.Table`.
type table struct {
	mu    *sync.RWMutex
	data  map[string][]byte
	codec db.Codec
}

// NewTable returns a new table using given Codec.
func NewTable(codec db.Codec) db.Table {
	return &table{
		mu:    new(sync.RWMutex),
		data:  map[string][]byte{},
		codec: codec,
	}
}

// Insert implements the `db.Table` interface.
func (table *table) Insert(key string, value interface{}) error {
	if key == "" {
		return db.ErrEmptyKey
	}

	table.mu.Lock()
	defer table.mu.Unlock()

	data, err := table.codec.Encode(value)
	if err != nil {
		return err
	}
	table.data[key] = data
	return nil
}

// Get implements the `db.Table` interface.
func (table *table) Get(key string, value interface{}) error {
	table.mu.RLock()
	defer table.mu.RUnlock()

	if key == "" {
		return db.ErrEmptyKey
	}
	val, ok := table.data[key]
	if !ok {
		return db.ErrKeyNotFound
	}
	return table.codec.Decode(val, value)
}

// Delete implements the `db.Table` interface.
func (table *table) Delete(key string) error {
	table.mu.Lock()
	defer table.mu.Unlock()

	delete(table.data, key)
	return nil
}

// Size implements the `db.Table` interface.
func (table *table) Size() (int, error) {
	table.mu.RLock()
	defer table.mu.RUnlock()

	return len(table.data), nil
}

// Iterator implements the `db.Table` interface.
func (table *table) Iterator() (db.Iterator, error) {
	table.mu.RLock()
	defer table.mu.RUnlock()

	return newIterator(DeepCopyMap(table.data), table.codec), nil
}

// iterator is a in-memory implementation of the `db.Iterator`.
type iterator struct {
	index  int
	keys   []string
	values [][]byte
	codec  db.Codec
}

// newIterator returns a `db.Iterator` with a
func newIterator(data map[string][]byte, codec db.Codec) db.Iterator {
	keys := make([]string, 0, len(data))
	values := make([][]byte, 0, len(data))
	for key, value := range data {
		keys = append(keys, key)
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

// DeepCopyMap returns a deep copy of a map[string][]byte.
func DeepCopyMap(m map[string][]byte) map[string][]byte {
	res := map[string][]byte{}
	for i, j := range m {
		res[i] = DeepCopyBytes(j)
	}
	return res
}

// DeepCopyBytes returns a deep copy of bytes slice.
func DeepCopyBytes(b []byte) []byte {
	res := make([]byte, len(b))
	for i := range res {
		res[i] = b[i]
	}
	return res
}

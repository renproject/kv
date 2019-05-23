package memdb

import (
	"sync"

	"github.com/renproject/kv/db"
)

// memdb is a in-memory implementation of the `db.Iterable`.
type memdb struct {
	mu   *sync.RWMutex
	data map[string][]byte
}

// New returns a new memdb.
func New() db.Iterable {
	return &memdb{
		mu:   new(sync.RWMutex),
		data: map[string][]byte{},
	}
}

// Insert implements the `db.Iterable` interface.
func (memdb memdb) Insert(key string, value []byte) error {
	memdb.mu.Lock()
	defer memdb.mu.Unlock()

	memdb.data[key] = value
	return nil
}

// Get implements the `db.Iterable` interface.
func (memdb memdb) Get(key string) ([]byte, error) {
	memdb.mu.RLock()
	defer memdb.mu.RUnlock()

	val, ok := memdb.data[key]
	if !ok {
		return nil, db.ErrNotFound
	}
	return val, nil
}

// Delete implements the `db.Iterable` interface.
func (memdb memdb) Delete(key string) error {
	memdb.mu.Lock()
	defer memdb.mu.Unlock()

	delete(memdb.data, key)
	return nil
}

// Size implements the `db.Iterable` interface.
func (memdb memdb) Size() (int, error) {
	memdb.mu.RLock()
	defer memdb.mu.RUnlock()

	return len(memdb.data), nil
}

// Iterator implements the `db.Iterable` interface.
func (memdb memdb) Iterator() db.Iterator {
	memdb.mu.RLock()
	defer memdb.mu.RUnlock()

	return newIterator(memdb.data)
}

type iterator struct {
	index  int
	keys   []string
	values [][]byte
}

func newIterator(data map[string][]byte) db.Iterator {
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
	}
}

// Next implements the `db.Iterator` interface.
func (iter *iterator) Next() bool {
	iter.index++
	return iter.index < len(iter.keys)
}

// Key implements the `db.Iterator` interface.
func (iter *iterator) Key() (string, error) {
	if iter.index == -1 {
		return "", db.ErrIndexOutOfRange
	}
	if iter.index >= len(iter.keys) {
		return "", db.ErrIndexOutOfRange
	}
	return iter.keys[iter.index], nil
}

// Value implements the `db.Iterator` interface.
func (iter *iterator) Value() ([]byte, error) {
	if iter.index == -1 {
		return nil, db.ErrIndexOutOfRange
	}
	if iter.index >= len(iter.keys) {
		return nil, db.ErrIndexOutOfRange
	}
	return iter.values[iter.index], nil
}

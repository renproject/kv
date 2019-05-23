package json

import (
	"encoding/json"

	"github.com/renproject/kv/db"
	"github.com/renproject/kv/store"
)

// iterable is a implementation of `store.Iterable` which using JSON for data
// encoding/decoding.
type iterable struct {
	db db.Iterable
}

// New returns a `store.Iterable`.
func New(db db.Iterable) store.Iterable {
	return &iterable{
		db: db,
	}
}

// Insert implements the `store.Iterable` interface.
func (store *iterable) Insert(key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return store.db.Insert(key, data)
}

// Get implements the `store.Iterable` interface.
func (store *iterable) Get(key string, value interface{}) error {
	data, err := store.db.Get(key)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, value)
}

// Delete implements the `store.Iterable` interface.
func (store *iterable) Delete(key string) error {
	return store.db.Delete(key)
}

// Size implements the `store.Iterable` interface.
func (store *iterable) Size() (int, error) {
	return store.db.Size()
}

// Iterator implements the `store.Iterable` interface.
func (store *iterable) Iterator() store.Iterator {
	iter := store.db.Iterator()
	return NewIterator(iter)
}

// iterable is a implementation of `store.Iterator` which using JSON for data
// encoding/decoding.
type iterator struct {
	iter db.Iterator
}

// NewIterator returns a new iterator.
func NewIterator(iter db.Iterator) store.Iterator {
	return &iterator{
		iter: iter,
	}
}

// Next implements the `store.Iterator` interface.
func (iter *iterator) Next() bool {
	return iter.iter.Next()
}

// Key implements the `store.Iterator` interface.
func (iter *iterator) Key() (string, error) {
	return iter.iter.Key()
}

// Value implements the `store.Iterator` interface.
func (iter *iterator) Value(value interface{}) error {
	data, err := iter.iter.Value()
	if err != nil {
		return err
	}
	return json.Unmarshal(data, value)
}

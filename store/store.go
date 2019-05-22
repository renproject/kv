package store

import (
	"fmt"
)

// ErrNotFound is returned when there is no value associated with a key.
var ErrNotFound = fmt.Errorf("not found")

// Store key-value tuples. The key must be a string and the value must be a byte
// slice.
type Store interface {
	// Insert a value associated with a key. This will overrride any existing
	// value associated with the key.
	Insert(key string, value []byte) error

	// Get the value associated with the key. If no value is associated with the
	// key, then an error will be returned.
	Get(key string) ([]byte, error)

	// Delete the value associated with the key.
	Delete(key string) error
}

// IterableStore is a Store that can iterate over its key-value tuples.
type IterableStore interface {
	Store

	// Size returns the number of key-value tuples in the IterableStore.
	Size() (int, error)

	// Iterator returns an Iterator which can be used to iterate throught all
	// key-value tuples in the IterableStore.
	Iterator() Iterator
}

// Iterator is used to iterate through the data in the store.
type Iterator interface {

	// Next will progress the iterator to the next element. If there are more
	// elements in the iterator, then it will return true, otherwise it will
	// return false.
	Next() bool

	// Key of the current key-value tuple.
	Key() (string, error)

	// Value of the current key-value tuple.
	Value() ([]byte, error)
}

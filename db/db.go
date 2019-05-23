package db

import (
	"errors"
)

// ErrNotFound is returned when there is no value associated with a key.
var ErrNotFound = errors.New("key not found")

// ErrEmptyKey is returned when key is empty.
var ErrEmptyKey = errors.New("key cannot be empty")

// ErrIndexOutOfRange is returned when the iterator index is not in a valid range.
var ErrIndexOutOfRange = errors.New("iterator index out of range")

// DB for storing key-value tuples. The key must be a string and the value must
// be a byte slice.
type DB interface {
	// Insert a value associated with a key. This will override any existing
	// value associated with the key.
	Insert(key string, value []byte) error

	// Get the value associated with the key. If no value is associated with the
	// key, then an `ErrNotFound` error will be returned.
	Get(key string) ([]byte, error)

	// Delete the value associated with the key.
	Delete(key string) error
}

// Iterable is a DB that can iterate over its key-value tuples.
type Iterable interface {
	DB

	// Size returns the number of key-value tuples in the Iterable DB.
	Size() (int, error)

	// Iterator returns an Iterator which can be used to iterate through all
	// key-value tuples in the IterableDB.
	Iterator() Iterator
}

// Iterator is used to iterate through the data in the store.
type Iterator interface {

	// Next will progress the iterator to the next element. If there are more
	// elements in the iterator, then it will return true, otherwise it will
	// return false.
	Next() bool

	// Key of the current key-value tuple. Calling Key() without calling
	// Next() or no next item in the iter will result in `ErrIndexOutOfRange`
	Key() (string, error)

	// Value of the current key-value tuple. Calling Value() without calling
	// Next() or no next item in the iter will result in `ErrIndexOutOfRange`
	Value() ([]byte, error)
}

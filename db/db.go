package db

import (
	"errors"
)

// ErrKeyNotFound is returned when there is no value associated with a key.
var ErrKeyNotFound = errors.New("key not found")

// ErrEmptyKey is returned when key is empty.
var ErrEmptyKey = errors.New("key cannot be empty")

// ErrIndexOutOfRange is returned when the iterator index is not in a valid
// range.
var ErrIndexOutOfRange = errors.New("iterator index out of range")

// Codec can do encoding/decoding between arbitrary data object and bytes.
type Codec interface {

	// Encode the object into a slice of bytes.
	Encode(obj interface{}) ([]byte, error)

	// Decode the bytes to its original data object and assign it to the given
	// variable. Value underlying `value` must be a pointer to the correct type
	// for object.
	Decode(data []byte, value interface{}) error
}

// DB is a key-value database which requires the key to be a string and the
// value can be encoded/decoded by the codec. It allows user to maintain
// multiple tables with the same underlying database driver.
type DB interface {

	// Close the DB and free all of its resources. The DB must not be used after
	// being closed.
	Close() error

	// Insert writes the key-value into the DB.
	Insert(key string, value interface{}) error

	// Get the value associated with the given key and write it to the value
	// interface. The value interface must be a pointer. If the key cannot be
	// found, then ErrKeyNotFound is returned.
	Get(key string, value interface{}) error

	// Delete the value with the given key from the DB.
	Delete(key string) error

	// Size returns the number of key/value pairs in the DB where the key begins
	// with the given prefix.
	Size(prefix string) (int, error)

	// Iterator over the key/value pairs in the DB where the key begins with the
	// given prefix.
	Iterator(prefix string) Iterator
}

// Iterator is used to iterate through the data in the store.
type Iterator interface {

	// Next will progress the iterator to the next element. If there are more
	// elements in the iterator, then it will return true, otherwise it will
	// return false.
	Next() bool

	// Key of the current key-value tuple. Calling Key() without calling Next()
	// or when no next item in the iter may result in `ErrIndexOutOfRange`
	Key() (string, error)

	// Value of the current key-value tuple. Calling Value() without calling
	// Next() or when no next item in the iter will result in
	// `ErrIndexOutOfRange`
	Value(value interface{}) error

	// Close must be called after finishing the iteration to release associated
	// resources. Close should always success and can be called multiple times
	// without causing error.
	Close()
}

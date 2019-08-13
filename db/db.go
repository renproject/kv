package db

import (
	"errors"
)

// ErrKeyNotFound is returned when there is no value associated with a key.
var ErrKeyNotFound = errors.New("key not found")

// ErrEmptyKey is returned when key is empty.
var ErrEmptyKey = errors.New("key cannot be empty")

// ErrIndexOutOfRange is returned when the iterator index is not in a valid range.
var ErrIndexOutOfRange = errors.New("iterator index out of range")

// Codec can do encoding/decoding between arbitrary data object and bytes.
type Codec interface {

	// Encode the object into a slice of bytes.
	Encode(obj interface{}) ([]byte, error)

	// Decode the bytes to its original data object and assign it to the given
	// variable. Value underlying `value` must be a pointer to the correct
	// type for object.
	Decode(data []byte, value interface{}) error
}

// Table is a sql-like table for storing key-value pairs. It requires the key
// to be a non-empty string and the value has the type which can be marshaled
// and unmarshaled by the used Codec.
type Table interface {

	// Insert writes the key-value into the table.
	Insert(key string, value interface{}) error

	// Get the value associated with the given key and assign it to the given
	// variable. This function returns ErrKeyNotFound if the key cannot be found.
	Get(key string, value interface{}) error

	// Delete the value with the given key from the table. It is safe to use
	// this function to delete a key which is not in the table.
	Delete(key string) error

	// Size returns the number of data entries in the table.
	Size() (int, error)

	// Iterator returns an iterator that can iterate over table.
	Iterator() (Iterator, error)
}

// DB is a collection of tables. It allows user to maintain multiple tables
// with the same underlying database driver. It will automatically creat a new
// table when first time writing to it.
type DB interface {

	// Insert the key-value pair into the table with given name. It will
	// return `ErrEmptyKey` if the key is empty.
	Insert(name string, key string, value interface{}) error

	// Get retrieves the value of given key from the specified table and unmarshals
	// it to the given variable. Value underlying `value` must be a pointer to
	// the correct type for object. It will return ErrTableNotFound if the table
	// doesn't exist. It will return `ErrEmptyKey` if the key is empty. It will
	// return `ErrKeyNotFound` if there's no value associated with the key.
	Get(name string, key string, value interface{}) error

	// Delete the data entry with given key from the specified table.
	Delete(name string, key string) error

	// Size returns the number of data entries in the given table.
	Size(name string) (int, error)

	// Iterator returns a iterator of the table with given name.
	Iterator(table string) (Iterator, error)
}

// Iterator is used to iterate through the data in the store.
type Iterator interface {

	// Next will progress the iterator to the next element. If there are more
	// elements in the iterator, then it will return true, otherwise it will
	// return false.
	Next() bool

	// Key of the current key-value tuple. Calling Key() without calling
	// Next() or when no next item in the iter may result in `ErrIndexOutOfRange`
	Key() (string, error)

	// Value of the current key-value tuple. Calling Value() without calling
	// Next() or when no next item in the iter will result in `ErrIndexOutOfRange`
	Value(value interface{}) error
}

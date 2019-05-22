package store

import (
	"fmt"
)

// ErrKeyNotFound is returned when there is no value associated with the key.
var ErrKeyNotFound = fmt.Errorf("key not found")

// Store is a generic key-value store. The key must be of type string, though there are no restrictions on the type
// of the value.
type Store interface {
	// Read the value associated with the given key. This function returns ErrKeyNotFound if the key cannot be found. I
	Read(key string, value interface{}) error

	// ReadData returns the raw bytes representation of the stored value.
	ReadData(key string) ([]byte, error)

	// Write writes the key-value into the store.
	Write(key string, value interface{}) error

	// WriteData writes the raw bytes representation of the value into the store.
	WriteData(key string, data []byte) error

	// Delete the entry with the given key from the store. It is safe to use this function to delete a key which is not
	// in the store.
	Delete(key string) error
}

// IterableStore is a Store which supports iterating.
type IterableStore interface {
	Store

	// Entries returns the number of data entries in the store.
	Entries() (int, error)

	// Iterator returns a KVStoreIterator which can be used to iterate though the data in the store at the time the
	// function is been called.
	Iterator() Iterator
}

// Iterator is used to iterate through the data in the store.
type Iterator interface {

	// Next tells if we reach the end of iterator.
	Next() bool

	// Key returns the key of the current key/value pair.
	Key() (string, error)

	// Value unmarshal the value of the current key/value pair into the given interface{}.
	Value(value interface{}) error
}

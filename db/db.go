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

// ErrTableAlreadyExists is returned when the table with given name is already exists in the db.
var ErrTableAlreadyExists = errors.New("table already exists")

// ErrTableNotFound is returned when there is no table with given name.
var ErrTableNotFound = errors.New("table not found")

// Codec can encode and decode between arbitrary data object and bytes.
type Codec interface {

	// Encode the object into a slice of bytes.
	Encode(obj interface{}) ([]byte, error)

	// Decode the bytes to its original data object and assign the object to the
	// given parameter. Value underlying `value` must be a pointer to the correct
	// type for object.
	Decode(data []byte, value interface{}) error
}

type Table interface {

	// Insert writes the key-value into the Table.
	Insert(key string, value interface{}) error

	// Get the value associated with the given key. This function returns
	// ErrKeyNotFound if the key cannot be found.
	Get(key string, value interface{}) error

	// Delete the value with the given key from the Table. It is safe to use
	// this function to delete a key which is not in the Table.
	Delete(key string) error

	// Size returns the number of data entries in the Table.
	Size() (int,error)

	// Iterator returns an iterator that can iterate over Table.
	Iterator() (Iterator, error)
}

// DB for storing key-value tuples. The key must be a string and the value must
// be a byte slice.
type DB interface {
	// Creates a new table in the DB with given name and Codec.
	NewTable(name string, codec Codec) (Table, error)

	// Table returns the table with the given name.
	Table(name string) (Table, error)

	// Insert the key, value pair into the table with given name. It will return
	// ErrTableNotFound if the table doesn't exist. It will return `ErrEmptyKey`
	// if the key is empty.
	Insert(table string, key string, value interface{}) error

	// Get retrieves the value of given key from the specified table and unmarshals
	// it to the given variable. Value underlying `value` must be a pointer to
	// the correct type for object. It will return ErrTableNotFound if the table
	// doesn't exist. It will return `ErrEmptyKey` if the key is empty. It will
	// return `ErrNotFound` if there's no value associated with the key.
	Get(table string, key string, value interface{}) error

	// Delete the data entry with given key from the specified table.
	Delete(table string, key string) error

	// Size returns the number of data entries in the given table.
	Size(name string) (int,error)

	// Iterator returns a iterator of the table with given name.
	Iterator(table string) (Iterator,error)
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
	Value(value interface{}) error
}

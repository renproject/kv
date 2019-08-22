package db

import (
	"fmt"

	"golang.org/x/crypto/sha3"
)

// Table is an abstraction over the DB that enforces a particular type of
// pattern in the key (i.e. same key prefix). It requires the key to be a
// non-empty string and the value can be encoded/decoded by the used Codec.
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
	Iterator() Iterator
}

type table struct {
	db       DB
	nameHash string
}

// NewTable creates a new table given name basing on provided DB.
func NewTable(db DB, name string) Table {
	hash := sha3.Sum256([]byte(name))
	return &table{
		db:       db,
		nameHash: string(hash[:]),
	}
}

// Insert implements the Table interface.
func (t *table) Insert(key string, value interface{}) error {
	return t.db.Insert(t.keyWithPrefix(key), value)
}

// Get implements the Table interface.
func (t *table) Get(key string, value interface{}) error {
	return t.db.Get(t.keyWithPrefix(key), value)
}

// Delete implements the Table interface.
func (t *table) Delete(key string) error {
	return t.db.Delete(t.keyWithPrefix(key))
}

// Size implements the Table interface.
func (t *table) Size() (int, error) {
	return t.db.Size(t.keyWithPrefix(""))
}

// Iterator implements the Table interface.
func (t *table) Iterator() Iterator {
	return t.db.Iterator(t.keyWithPrefix(""))
}

// keyWithPrefix formats the key with table hash.
func (t *table) keyWithPrefix(key string) string {
	return fmt.Sprintf("%v_%v", t.nameHash, key)
}

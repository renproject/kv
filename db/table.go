package db

import (
	"fmt"

	"golang.org/x/crypto/sha3"
)

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
	Iterator() Iterator
}

type table struct {
	db       DB
	nameHash string
}

func NewTable(db DB, name string) Table {
	hash := sha3.Sum256([]byte(name))
	return &table{
		db:       db,
		nameHash: string(hash[:]),
	}
}

func (t *table) Insert(key string, value interface{}) error {
	return t.db.Insert(t.keyWithPrefix(key), value)
}

func (t *table) Get(key string, value interface{}) error {
	return t.db.Get(t.keyWithPrefix(key), value)
}

func (t *table) Delete(key string) error {
	return t.db.Delete(t.keyWithPrefix(key))
}

func (t *table) Size() (int, error) {
	return t.db.Size(t.keyWithPrefix(""))
}

func (t *table) Iterator() Iterator {
	return t.db.Iterator(t.keyWithPrefix(""))
}

func (t *table) keyWithPrefix(key string) string {
	return fmt.Sprintf("%v_%v", t.nameHash, key)
}

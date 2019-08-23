package db

import (
	"fmt"

	"golang.org/x/crypto/sha3"
)

// A Table is an abstraction over the DB that partitions key/value pairs. The
// Table name must be unique compared to other Table names. The key/value pairs
// are encoded and decoded using the Codec of the underlying DB.
type Table interface {

	// Insert writes the key-value into the Table.
	Insert(key string, value interface{}) error

	// Get the value associated with the given key and write it to the value
	// interface. The value interface must be a pointer. If the key cannot be
	// found, then ErrKeyNotFound is returned.
	Get(key string, value interface{}) error

	// Delete the value with the given key from the Table.
	Delete(key string) error

	// Size returns the number of key/value pairs in the Table.
	Size() (int, error)

	// Iterator over the key/value pairs in the Table.
	Iterator() Iterator
}

type table struct {
	db       DB
	nameHash string
}

// NewTable creates a new Table with the given name. If the underlying DB is
// safe for concurrent use, then the Table is safe for concurrent use.
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

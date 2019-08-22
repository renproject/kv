// Package kv defines a standard interface for key-value stores and key-value
// iterators. It provides persistent implementations using BadgerDB. It provides
// non-persistent implementations using a concurrent-safe in-memory map.
package kv

import (
	"errors"

	"github.com/renproject/kv/badgerdb"
	"github.com/renproject/kv/cache/lru"
	"github.com/renproject/kv/cache/ttl"
	"github.com/renproject/kv/codec"
	"github.com/renproject/kv/db"
	"github.com/renproject/kv/leveldb"
	"github.com/renproject/kv/memdb"
)

var (
	// ErrKeyNotFound is returned when there is no value associated with a key.
	ErrKeyNotFound = errors.New("key not found")

	// ErrEmptyKey is returned when key is empty.
	ErrEmptyKey = errors.New("key cannot be empty")

	// ErrIndexOutOfRange is returned when the iterator index is not in a valid range.
	ErrIndexOutOfRange = errors.New("iterator index out of range")

	// ErrTableAlreadyExists is returned when the table with given name is already exists in the db.
	ErrTableAlreadyExists = errors.New("table already exists")

	// ErrTableNotFound is returned when there is no table with given name.
	ErrTableNotFound = errors.New("table not found")
)

type (
	// Table is an abstraction over the DB that enforces a particular type of
	// pattern in the key (i.e. same key prefix). It requires the key to be a
	// non-empty string and the value can be encoded/decoded by the used Codec.
	Table = db.Table

	// DB is able to add new table and does operations on certain table by its name.
	DB = db.DB

	// Codec can encode and decode between arbitrary data object and bytes.
	Codec = db.Codec

	// Iterator is used to iterate through the data in the store.
	Iterator = db.Iterator
)

// Codecs
var (
	// JSONCodec is a json codec that marshals and unmarshals values using the
	// standard Golang JSON marshalers. For more information, see
	// https://golang.org/pkg/encoding/json.
	JSONCodec = codec.JSONCodec

	// GobCodec is a gob codec that encodes and decodes values using gob. For
	// more information, see https://golang.org/pkg/encoding/gob.
	GobCodec = codec.GobCodec
)

// Initializing DB and table
var (
	// NewMemDB returns a key-value database that is implemented in-memory. This
	// implementation is fast, but does not store data on-disk. It is safe for
	// concurrent use.
	NewMemDB = memdb.New

	// NewBadgerDB returns a key-value database that is implemented using
	// BadgerDB. For more information, see https://github.com/dgraph-io/badger.
	NewBadgerDB = badgerdb.New

	// NewLevelDB returns a key-value database that is implemented using
	// levelDB. For more information, see https://github.com/syndtr/goleveldb.
	NewLevelDB = leveldb.New

	// NewTable returns a new table basing on the given DB and codec.
	NewTable = db.NewTable
)

// Table wrappers
var (
	// NewLRUTable wraps a given Table and creates a Table which has lru cache.
	NewLRUTable = lru.NewLruTable

	// NewTTLCache wraps a given DB and creates a time-to-live DB. It will
	// automatically prune the data in the db until the context expires.
	NewTTLCache = ttl.New
)

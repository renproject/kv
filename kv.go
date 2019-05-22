// Package kv defines a standard interface for key-value stores and key-value
// iterators. It provides persistent implementations using BadgerDB and LevelDB.
// It provides non-persistent implementations using a concurrent-safe in-memory
// map.
package kv

import (
	"github.com/renproject/kv/badgerdb"
	"github.com/renproject/kv/db"
	"github.com/renproject/kv/json"
	"github.com/renproject/kv/leveldb"
	"github.com/renproject/kv/memdb"
)

var (
	// ErrNotFound is returned when there is no value associated with a key.
	ErrNotFound = db.ErrKeyNotFound
)

var (
	DB = db.DB

	IterableDB = db.IterableDB

	// Iterator defines a standard interface for iterating over key-value pairs
	// from an IterableStore.
	Iterator = db.Iterator
)

var (
	// NewBadgerDB returns a key-value database that is implemented using
	// BadgerDB. For more information, see https://github.com/dgraph-io/badger.
	NewBadgerDB = badgerdb.New

	// NewLevelDB returns a key-value database that is implemented using
	// LevelDB. It is recommended that new applications use BadgerDB. For more
	// information, see https://github.com/syndtr/goleveldb/leveldb.
	NewLevelDB = leveldb.New

	// NewMemDB returns a key-value database that is implemented in-memory. This
	// implementation is fast, but should not be used for persistent data
	// storage, and does not support iteration.
	NewMemDB = memdb.New
)

var (
	// NewJSON returns a key-value store that marshals and unmarshals keys using
	// the standard Golang JSON marshalers. For more information, see
	// https://golang.org/pkg/encoding/json.
	NewJSON = json.NewStore
)

// Package kv defines a standard interface for key-value stores and key-value
// iterators. It provides persistent implementations using BadgerDB and LevelDB.
// It provides non-persistent implementations using a concurrent-safe in-memory
// map.
package kv

import (
	"github.com/renproject/kv/badgerdb"
	"github.com/renproject/kv/leveldb"
	"github.com/renproject/kv/store"
)

var (
	// ErrNotFound is returned when there is no value associated with a key.
	ErrNotFound = store.ErrKeyNotFound
)

var (
	// Store defines a standard interface for reading and writing data to a
	// key-value store.
	Store = store.Store

	// IterableStore defines a standard interface for a Store that can iterate
	// over its key-value pairs.
	IterableStore = store.IterableStore

	// Iterator defines a standard interface for iterating over key-value pairs
	// from an IterableStore.
	Iterator = store.Iterator
)

var (
	// NewBadgerDB returns a key-value store that is implemented using BadgerDB.
	// For more information, see https://github.com/dgraph-io/badger.
	NewBadgerDB = badgerdb.New

	// NewLevelDB returns a key-value store that is implemented using LevelDB.
	// It is recommended that new applications use BadgerDB. For more
	// information, see github.com/syndtr/goleveldb/leveldb.
	NewLevelDB = leveldb.New

	// NewMemDB returns a key-value store that is implement in-memory. This
	// implementation is fast, but should not be used for persistent data
	// storage.
	NewMemDB = memdb.New
)

// Package kv defines a standard interface for key-value stores and key-value
// iterators. It provides persistent implementations using BadgerDB and LevelDB.
// It provides non-persistent implementations using a concurrent-safe in-memory
// map.
package kv

import (
	"github.com/renproject/kv/badgerdb"
	"github.com/renproject/kv/db"
	"github.com/renproject/kv/gob"
	"github.com/renproject/kv/json"
	"github.com/renproject/kv/leveldb"
	"github.com/renproject/kv/memdb"
	"github.com/renproject/kv/store"
)

var (
	// ErrNotFound is returned when there is no value associated with a key.
	ErrNotFound = db.ErrNotFound
)

var (
	// TODO: Comment!
	Store = store.Store

	// TODO: Comment!
	Iterable = store.Iterable

	// TODO: Comment!
	Iterator = store.Iterator
)

var (
	// NewJSON returns a key-value store that marshals and unmarshals values
	// using the standard Golang JSON marshalers. For more information, see
	// https://golang.org/pkg/encoding/json.
	NewJSON = json.NewStore

	// NewGob returns a key-value store that encodes and decodes values using
	// gob. For more information, see https://golang.org/pkg/encoding/gob.
	NewGob = gob.NewStore
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

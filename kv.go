// Package kv defines a standard interface for key-value stores and key-value
// iterators. It provides persistent implementations using BadgerDB and LevelDB.
// It provides non-persistent implementations using a concurrent-safe in-memory
// map.
package kv

import (
	"github.com/renproject/kv/badgerdb"
	"github.com/renproject/kv/cache"
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

type (
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
	NewJSON = json.New

	// NewGob returns a key-value store that encodes and decodes values using
	// gob. For more information, see https://golang.org/pkg/encoding/gob.
	NewGob = gob.New
)

var (
	// NewTTLCache returns a cache that wraps an underlying store. Keys that have
	// no been accessed for the specified duration will be automatically deleted
	// from the underlying store. It is safe for concurrent use, as long as the
	// underlying store is also safe for concurrent use.
	NewTTLCache = cache.New
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
	// implementation is fast, but does not store data on-disk. A time-to-live can
	// be used to automatically delete key-value tuples after they have been in the
	// database for more than a specific duration. A time-to-live of zero will keep
	// key-value tuples until they are explicitly deleted. It is safe for concurrent
	// use.
	NewMemDB = memdb.New
)

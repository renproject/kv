// Package kv defines a standard interface for key-value storage and iteration.
// It supports persistent storage using LevelDB and BadgerDB. It also supports
// non-persistent storage using concurrent-safe in-memory maps.
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

	// ErrEmptyKey is returned when a key is the empty string.
	ErrEmptyKey = errors.New("key cannot be empty")

	// ErrIndexOutOfRange is returned when the iterator index is less than zero,
	// or, greater than or equal to the size of the iterator.
	ErrIndexOutOfRange = errors.New("iterator index out of range")
)

type (
	// A Table is an abstraction over the DB that partitions key/value pairs. The
	// Table name must be unique compared to other Table names.
	Table = db.Table

	// A DB is a low-level interface for storing and iterating over key/value
	// pairs.
	DB = db.DB

	// A Codec defines an encoding/decoding between values and bytes.
	Codec = db.Codec

	// An Iterator is used to lazily iterate over key/value pairs.
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

var (
	// NewLRUTable wraps a given Table and creates a Table which has lru cache.
	NewLRUTable = lru.NewLruTable

	// NewTTLCache wraps a given DB and creates a time-to-live DB. It will
	// automatically prune the data in the db until the context expires.
	NewTTLCache = ttl.New
)

// Package kv defines a standard interface for key-value stores and key-value
// iterators. It provides persistent implementations using BadgerDB. It provides
// non-persistent implementations using a concurrent-safe in-memory map.
package kv

import (
	"errors"

	"github.com/renproject/kv/badgerdb"
	"github.com/renproject/kv/codec"
	"github.com/renproject/kv/db"
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
	// TODO: Comment!
	Table = db.Table

	// TODO: Comment!
	DB = db.DB

	// TODO: Comment!
	Codec = db.Codec

	// TODO: Comment!
	Iterator = db.Codec
)

// In-memory implementation of the DB and table
var (

	// TODO: Comment!
	NewMemTable = memdb.NewTable


	// NewMemDB returns a key-value database that is implemented in-memory. This
	// implementation is fast, but does not store data on-disk. A time-to-live can
	// be used to automatically delete key-value tuples after they have been in the
	// database for more than a specific duration. A time-to-live of zero will keep
	// key-value tuples until they are explicitly deleted. It is safe for concurrent
	// use.
	NewMemDB = memdb.New
)

// BadgerDB implementation of the DB and table
var (
	// TODO: Comment!
	NewBadgerTable = badgerdb.NewTable

	// NewBadgerDB returns a key-value database that is implemented using
	// BadgerDB. For more information, see https://github.com/dgraph-io/badger.
	NewBadgerDB = badgerdb.New
)

var (
	// JsonCodec is a json codec that marshals and unmarshals values using the
	// standard Golang JSON marshalers. For more information, see
	// https://golang.org/pkg/encoding/json.
	JsonCodec = codec.JsonCodec

	// GobCodec is a gob codec that encodes and decodes values using gob. For
	// more information, see https://golang.org/pkg/encoding/gob.
	GobCodec = codec.GobCodec
)

// var (
// 	// NewTTLCache returns a cache that wraps an underlying store. Keys that have
// 	// no been accessed for the specified duration will be automatically deleted
// 	// from the underlying store. It is safe for concurrent use, as long as the
// 	// underlying store is also safe for concurrent use.
// 	NewTTLCache = cache.NewTTL
// )


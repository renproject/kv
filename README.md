# `📦 kv`

[![GoDoc](https://godoc.org/github.com/renproject/kv?status.svg)](https://godoc.org/github.com/renproject/kv)
![](https://github.com/renproject/kv/workflows/Test/badge.svg)
![Go Report](https://goreportcard.com/badge/github.com/renproject/kv)
[![Coverage Status](https://coveralls.io/repos/github/renproject/kv/badge.svg?branch=master)](https://coveralls.io/github/renproject/kv?branch=master)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](https://opensource.org/licenses/MIT)

A flexible and extensible library for key-value storage.

- [x] Multiple encoding/decoding formats
- [x] Persistent database drivers
- [x] In-memory database drivers
- [x] Time-to-live caching
- [x] Safe for concurrent use
- [x] Production ready

Installation
------------

```sh
go get github.com/renproject/kv
```

Requirements
------------

Requires `go1.12` or newer.

Usage
-----

### Codec

A `Codec` encodes `interface{}` values into bytes, decode bytes into the `interface{}` values. Generally, when a specific type is not supported, a `Codec` will panic. Out of the box, KV supports:

- `JSONCodec` which encodes/decodes using the standard library [JSON](https://golang.org/pkg/encoding/json) marshaler, and
- `GobCodec` which encodes/decodes using the standard library [Gob]https://golang.org/pkg/encoding/gob marshaler (you must explicitly register types outside of KV).

An example of using the `JSONCodec`:

```go
db := kv.NewLevelDB(".db", kv.JSONCodec)
```

### DB

A `DB` is a key/value database. The key is a `string` and the value is an `interface{}` that can be encoded/decoded by the chosen `Codec` (different `DBs` can use different `Codecs`). A `DB` is safe for concurrent if, and only if, the underlying driver is safe for concurrent use (the LevelDB driver and BadgerDB driver are safe for concurrent use).

An example of initialising a `DB`:

```go
// Initialising an in-memory database 
db := kv.NewMemDB(kv.JSONCodec)

// Initialising a LevelDB database
db = kv.NewLevelDB(".ldb", kv.JSONCodec)

// Initialising a BadgerDB database 
db = kv.NewBadgerDB(".bdb", kv.JSONCodec)
```

Although reading/writing is usually done through a `Table`, you can read/write using the `DB` directly (you must be careful that keys will not conflict with `Table` name hashes):

```go
// Write
if err := db.Insert("key", "value"); err != nil {
    log.Fatalf("error inserting: %v", err)
}

// Read
var value string
if err := db.Get("key", &value); err != nil {
    log.Fatalf("error getting: %v", err)
}

// Delete
if err := db.Delete("key"); err != nil {
    log.Fatalf("error deleting: %v", err)
}

// Number of key/value pairs with the given prefix
size, err := db.Size("")
if err != nil {
    log.Fatalf("error sizing: %v", err)
}
log.Printf("%v key/value pairs found", size)

// Get an iterator over all key/value pairs with the given prefix
iter := db.Iterator("")
```


### Table

A `Table` is an abstraction over a `DB` partitions key/value pairs into non-overlapping groups. This allows you to iterate over small groups of key/value pairs that are logically related. You must ensure that `Table` names are unique.

An example of basic use:

```go
type Foo struct{
    A string
    B int
    C []byte
}

// Init
table := kv.NewTable(db, "myAwesomeTable")

// Write
foo := Foo{"foo", 420, []byte{1,2,3}}
if err := table.Insert("key", foo); err != nil {
    log.Fatalf("error inserting into table: %v", err)
}

// Read
bar := Foo{}
if err := table.Get("key", &bar); err != nil {
    log.Fatalf("error getting from table: %v", err)
}
```

The most useful feature of `Tables` is iteration:

```go
// Get the number of key/value pairs in the table
size, err := table.Size()
if err != nil {
    log.Fatalf("error sizing table: %v", err)
}
log.Printf("%v key/value pairs found", size)

// Iterate over all key/value pairs in the table
for iter := table.Iterator(); iter.Next(); {
    key, err := iter.Key()
    if err != nil {
        continue
    }
    value := Foo{}
    if err = iter.Value(&value); err != nil {
        continue
    }
}
```

Benchmarks
----------

| Database | Number of iterations run | Time (ns/op) | Memory (bytes/op) |
|----------|:------------------------:|-------------:|------------------:|
| LevelDB  |           2000           |     10784337 |   4397224         |
| BadgerDB |            100           |    200012411 | 200012411         |

Contributors
------------

Built with ❤ by Ren.

# KV

[![CircleCI](https://circleci.com/gh/renproject/kv/tree/master.svg?style=shield)](https://circleci.com/gh/renproject/kv/tree/master)
![Go Report](https://goreportcard.com/badge/github.com/renproject/kv)
[![Coverage Status](https://coveralls.io/repos/github/renproject/kv/badge.svg?branch=master)](https://coveralls.io/github/renproject/kv?branch=master)

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

Requires `go1.6` or newer.

Usage
-----

### Codec

A `Codec` is encodes `interface{}` values into bytes, decode bytes into the `interface{}` values. Generally, when a specific type is not supported, a `Codec` will panic. Out of the box, KV supports:

- `JSONCodec` which encodes/decodes using the standard library [JSON](https://golang.org/pkg/encoding/json) marshaler, and
- `GobCodec` which encodes/decodes using the standard library [Gob]https://golang.org/pkg/encoding/gob marshaler (you must explicitly register types outside of KV).

An example of using the `JSONCodec`:

```go
// Init LevelDB
ldb, err := leveldb.OpenFile("./db", nil)
if err != nil {
    log.Fatalf("error opening ldb file: %v", err)
}
// Init KV
db := kv.NewLevelDB(kv.JsonCodec)
```

### Table

A `Table` groups key/value pairs by automatically prefixing the key with a table name. Inserting a key/value pair into a table requires the key to be non-empty.

Creating a Table:

```go
// In-memory implementation 
table := kv.NewMemTable(kv.JsonCodec)

// Leveldb implementation
ldb, err = leveldb.OpenFile("./.leveldb", nil)
handle(err)
table := kv.NewLevelTable("name", ldb, kv.JsonCodec)

// BadgerDB implementation 
bdb, err:= badger.Open(badger.DefaultOptions("."))
handle(err)
table := kv.NewBadgerTable("name", bdb, kv.JsonCodec)
```

Read, write and delete on a table :

```go
    type Ren struct{
        A string
        B int
        C []byte
    }
    
    // Insert new data 
    ren := Ren{ "ren", 100, []byte{1,2,3}}
    err := table.Insert("key", ren)
    handle(err)
    
    // Retrieve data 
    var newRen Ren
    err = table.Get("key", &newRen) // Make sure you pass a pointer here
    handle(err)
    fmt.Printf("old ren = %v\nnew ren = %v", ren, newRen)
    // old ren = {ren 100 [1 2 3]}
    // new ren = {ren 100 [1 2 3]} 	

    // Delete data
    err := table.Delete("key")
    handle(err)
```

Iterating through the table 
```go
    // The iterator will not be able to return data added after the iterator been created 
    iter, err:= table.Iterator()
    handle(err)
    
    for iter.Next(){
        key, err := iter.Key()
        handle(err)
        var value Ren 
        err = iter.Value(&value)  // Make sure you pass a pointer here
        handle(err)
    }
```

### DB
DB is a collection of tables. It is useful when you want to have multiple tables and use the same underlying database instance. (i.e. same badgerDB file). You can create new tables in the DB or accessing existing table by the table name.
DB is also concurrent safe to use as long as the underlying implementation is. There're helper functions which allow you to manipulate on
a specific table of the DB directly. Or your can get the table by it's name and calling functions from the table.

Creating a DB:
```go
	// In-memory implementation 
	db := kv.NewMemDB()

    // LevelDB implementation 
    ldb, err = leveldb.OpenFile("./.leveldb", nil)
    handle(err)
    db := kv.NewLevelDB(ldb, kv.JsonCodec)

	// BadgerDB implementation 
	bdb, err:= badger.Open(badger.DefaultOptions("."))
	handle(err)
	db := kv.NewBadgerDB(bdb, kv.JsonCodec)
	

```

Read/Write directly though the DB. (It will initialize an empty table if the table of given name doesn't exist.)
```go
	db := kv.NewBadgerDB(bdb)
	err = db.Insert("name", "key", "value")
	handle(err)
	var value string
	err = db.Get("name", "key", &value)
	handle(err)
	err = db.Delete("name", "key")
	handle(err)
	size, err := db.Size("name")
	handle(err)
	iter, err := db.Iterator("name")
	handle(err)
```

### Benchmarks results

| Database | Number of iterations run | Time (ns/op) | Memory (bytes/op) |
|----------|:------------------------:|-------------:|-------------------|
| LevelDB  |           2000           |     10784337 | 4397224           |
| BadgerDB |            100           |    200012411 | 200012411         |

Contributors
------------

Built with ❤ by Ren.

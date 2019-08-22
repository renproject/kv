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

Installation
-----------

	go get github.com/renproject/kv

Requirements
-----------

* Need at least `go1.6` or newer.

Usage
-----------

### Codec

A **Codec** is something can encode arbitrary object into bytes and decode bytes back to the original object.
There're two **Codec** we currently support `JsonCodec` and `GobCodec`.
More details can be found from [JsonCodec](https://golang.org/pkg/encoding/json/) and [GobCodec](https://golang.org/pkg/encoding/gob/)

```go
    codec := kv.JSONCodec
    
    codec := kv.GobCodec

```

### DB
DB is a key-value database which requires the key to be a string and the value can be encoded/decoded by the codec. 
DB is also concurrent safe to use as long as the underlying implementation is. People can create multiple tables 
using the safe DB. 

Creating a DB:
```go
	// In-memory implementation 
	db := kv.NewMemDB(kv.JSONCodec)

    // LevelDB implementation
    db := kv.NewLevelDB("./.leveldb", kv.JsonCodec)

	// BadgerDB implementation 
	db := kv.NewBadgerDB("./.badgerdb", kv.JsonCodec)

```

Read/Write directly though the DB. (It will initialize an empty table if the table of given name doesn't exist.)
```go
	err := db.Insert("key", "value")
	handle(err)
	var value string
	err := db.Get("key", &value)
	handle(err)
	err := db.Delete("key")
	handle(err)

	size, err := db.Size("") // calling size will empty prefix returns the total size of the db.
	handle(err)
	iter := db.Iterator("prefix")
```


### Table

Table is an abstraction over the DB that enforces a particular type of pattern in the key (i.e. same key prefix). 
It requires the key to be a non-empty string and the value can be encoded/decoded by the used Codec.

Usage:

```go
    type Ren struct{
        A string
        B int
        C []byte
    }

    // Initialize a table with given name and DB
    table := kv.NewTable(db, "table_name")

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
    // Get size of the table 
    size, err:= table.Size()
    handle(err)

    // The iterator will not be able to return data added after the iterator been created 
    iter := table.Iterator()    
    for iter.Next(){
        key, err := iter.Key()
        handle(err)
        var value Ren 
        err = iter.Value(&value)  // Make sure you pass a pointer here
        handle(err)
    }
```


### Benchmarks results

| Database | Number of iterations run | Time (ns/op) | Memory (bytes/op) |
|----------|:------------------------:|-------------:|-------------------|
| LevelDB  |           2000           |     10784337 | 4397224           |
| BadgerDB |            100           |    200012411 | 200012411         |

Built with ❤ by Ren.

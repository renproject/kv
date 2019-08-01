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

###Codec###

A **Codec** is something can encode arbitrary object into bytes and decode bytes back to the original object.
There're two **Codec** we currently support `JsonCodec` and `GobCodec`.
More details can be found from [JsonCodec](https://golang.org/pkg/encoding/json/) and [GobCodec](https://golang.org/pkg/encoding/gob/)

```go
    codec := kv.JsonCodec
    codec = kv.GobCodec

```

###Table###
A **Table** is a sql-like table for storing key-value pairs.
It requires the key to be a non-empty string and the value to be able to be marshaled/unmarshaled by the provided **Codec**.

Creating a Table:
```go
	// BadgerDB implementation 
	bdb, err:= badger.Open(badger.DefaultOptions("."))
	handle(err)
	badgerTable := kv.NewBadgerTable("table_name", bdb, kv.JsonCodec)
	
	// In-memory implementation 
	cacheTable := kv.NewMemTable(kv.JsonCodec)

```

Read write and delete on a table :

```go
    type Ren struct{
        A string
        B int
        C []byte
    }
   
    ren := Ren{ "ren", 100, []byte{1,2,3}}
    err := table.Insert("key", ren)
    handle(err)
    
    var newRen Ren
    err = table.Get("key", &newRen) // Make sure you provide a pointer here
    handle(err)
    
    fmt.Printf("old ren = %v\nnew ren = %v", ren, newRen)
    // old ren = {ren 100 [1 2 3]}
    // new ren = {ren 100 [1 2 3]} 	

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
        err = iter.Value(&value)  // Make sure you provide a pointer here
        handle(err)
    }
```

###DB###
DB is a collection of tables. You can create new tables in the DB or accessing existing table by the table name.
DB is also concurrent safe to use as long as the table is. There're helper functions which allow you to manipulate on
a specific table of the DB directly. Or your can get the table by it's name and calling functions from the table.

Creating a DB:
```go
	// BadgerDB implementation 
	bdb, err:= badger.Open(badger.DefaultOptions("."))
	handle(err)
	badgerDB := kv.NewBadgerDB( bdb)
	
	// In-memory implementation 
	cacheDB := kv.NewMemDB()
```

Creating new tables or accessing existing ones 
```go
	table, err := db.NewTable("table-name", kv.JsonCodec)
	handle(err)
	
	table, err = db.Table("table-name")

```

Read/Write directly though the DB 
```go
	db := kv.NewBadgerDB(bdb)
	_, err := db.NewTable("table_name", kv.JsonCodec)
	handle(err)
	err = db.Insert("table_name", "key", "value")
	handle(err)
	var value string
	err = db.Get("table_name", "key", &value)
	handle(err)
	err = db.Delete("table_name", "key")
	handle(err)
	size, err := db.Size("table_name")
	handle(err)
	iter, err := db.Iterator("table_name")
	handle(err)
```




Built with ❤ by Ren.

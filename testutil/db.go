package testutil

import (
	"github.com/renproject/kv/badgerdb"
	"github.com/renproject/kv/codec"
	"github.com/renproject/kv/db"
	"github.com/renproject/kv/leveldb"
	"github.com/renproject/kv/memdb"
)

// Codecs we want to test.
var Codecs = []db.Codec{
	codec.JSONCodec,
	codec.GobCodec,
}

var DbInitalizer = []func(db.Codec) db.DB{
	func(codec db.Codec) db.DB {
		return memdb.New(codec)
	},
	func(codec db.Codec) db.DB {
		return leveldb.New(".leveldb", codec)
	},
	func(codec db.Codec) db.DB {
		return badgerdb.New(".badgerdb", codec)
	},
}

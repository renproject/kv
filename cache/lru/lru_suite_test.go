package lru_test

import (
	"os/exec"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/renproject/kv/badgerdb"
	"github.com/renproject/kv/codec"
	"github.com/renproject/kv/db"
	"github.com/renproject/kv/leveldb"
	"github.com/renproject/kv/memdb"
)

func TestLru(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Lru Suite")
}

// Codecs we want to test.
var codecs = []db.Codec{
	codec.JSONCodec,
	codec.GobCodec,
}

var dbInitalizer = []func(db.Codec) db.DB{
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

// Clean the badgerDB instance after each test
var _ = JustAfterEach(func() {
	Expect(exec.Command("rm", "-rf", "./.badgerdb").Run()).NotTo(HaveOccurred())
	Expect(exec.Command("rm", "-rf", "./.leveldb").Run()).NotTo(HaveOccurred())
})

package kv_test

import (
	"math/rand"
	"os/exec"
	"reflect"
	"testing"

	"github.com/dgraph-io/badger"
	bdb "github.com/renproject/kv/badgerdb"
	"github.com/renproject/kv/cache/lru"
	"github.com/renproject/kv/codec"
	"github.com/renproject/kv/db"
	ldb "github.com/renproject/kv/leveldb"
	"github.com/renproject/kv/testutil"
	"github.com/syndtr/goleveldb/leveldb"

	. "github.com/onsi/gomega"
)

const (
	benchmarkReads  = 10000
	benchmarkWrites = 1000
	cacheLimit      = 100
)

func BenchmarkLevelDB(b *testing.B) {
	for n := 0; n < b.N; n++ {
		func() {
			leveldb := initLevelDB()
			defer closeLevelDB(leveldb)

			lDB := ldb.New(leveldb, codec.JsonCodec)
			benchmarkDB(lDB)
		}()
	}
}

func BenchmarkLevelDBWithLRUCache(b *testing.B) {
	for n := 0; n < b.N; n++ {
		func() {
			leveldb := initLevelDB()
			defer closeLevelDB(leveldb)

			lru := lru.New(ldb.New(leveldb, codec.JsonCodec), cacheLimit)
			benchmarkDB(lru)
		}()
	}
}

func BenchmarkBadgerDB(b *testing.B) {
	for n := 0; n < b.N; n++ {
		func() {
			badgerDB := initBadgerDB()
			defer closeBadgerDB(badgerDB)

			bDB := bdb.New(badgerDB, codec.JsonCodec)
			benchmarkDB(bDB)
		}()
	}
}

func BenchmarkBadgerDBWithLRUCache(b *testing.B) {
	for n := 0; n < b.N; n++ {
		func() {
			badgerDB := initBadgerDB()
			defer closeBadgerDB(badgerDB)

			lru := lru.New(bdb.New(badgerDB, codec.JsonCodec), cacheLimit)
			benchmarkDB(lru)
		}()
	}
}

func benchmarkDB(database db.DB) {
	name := "testDB"
	key := "testKey"

	vals := make([]testutil.TestStruct, benchmarkWrites)

	for i := 0; i < benchmarkWrites; i++ {
		newKey := key + string(i)
		vals[i] = testutil.RandomTestStruct()
		Expect(database.Insert(name, newKey, vals[i])).NotTo(HaveOccurred())
	}

	for i := 0; i < benchmarkReads; i++ {
		queryIndex := rand.Intn(benchmarkWrites)
		queryKey := key + string(queryIndex)
		val := testutil.TestStruct{D: []byte{}}
		err := database.Get(name, queryKey, &val)
		Expect(err).NotTo(HaveOccurred())
		Expect(reflect.DeepEqual(val, vals[queryIndex])).Should(BeTrue())
	}
}

func initBadgerDB() *badger.DB {
	Expect(exec.Command("mkdir", "-p", ".badgerdb").Run()).NotTo(HaveOccurred())
	opts := badger.DefaultOptions("./.badgerdb")
	opts.Dir = "./.badgerdb"
	opts.ValueDir = "./.badgerdb"
	db, err := badger.Open(opts.WithLogger(nil))
	Expect(err).NotTo(HaveOccurred())
	return db
}

func closeBadgerDB(db *badger.DB) {
	Expect(db.Close()).NotTo(HaveOccurred())
	Expect(exec.Command("rm", "-rf", "./.badgerdb").Run()).NotTo(HaveOccurred())
}

func initLevelDB() *leveldb.DB {
	db, err := leveldb.OpenFile("./.leveldb", nil)
	Expect(err).NotTo(HaveOccurred())
	return db
}

func closeLevelDB(db *leveldb.DB) {
	Expect(db.Close()).NotTo(HaveOccurred())
	Expect(exec.Command("rm", "-rf", "./.leveldb").Run()).NotTo(HaveOccurred())
}

func randBytes() []byte {
	data := make([]byte, rand.Intn(100))
	rand.Read(data)
	return data[:]
}

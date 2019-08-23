package kv_test

import (
	"math/rand"
	"reflect"
	"testing"

	. "github.com/onsi/gomega"

	"github.com/renproject/kv/badgerdb"
	"github.com/renproject/kv/cache/lru"
	"github.com/renproject/kv/codec"
	"github.com/renproject/kv/db"
	"github.com/renproject/kv/leveldb"
	"github.com/renproject/kv/testutil"
)

const (
	benchmarkReads  = 10000
	benchmarkWrites = 1000
	cacheLimit      = 100
)

func BenchmarkLevelDB(b *testing.B) {
	for n := 0; n < b.N; n++ {
		func() {
			lDB := leveldb.New(".leveldb", codec.JSONCodec)
			defer lDB.Close()

			benchmarkDB(lDB)
		}()
	}
}

func BenchmarkLevelDBWithLRUCache(b *testing.B) {
	for n := 0; n < b.N; n++ {
		func() {
			lDB := leveldb.New(".leveldb", codec.JSONCodec)
			defer lDB.Close()

			table := lru.NewLruTable(db.NewTable(lDB, "lru"), cacheLimit)
			benchmarkTable(table)
		}()
	}
}

func BenchmarkBadgerDB(b *testing.B) {
	for n := 0; n < b.N; n++ {
		func() {
			badgerDB := badgerdb.New(".badgerdb", codec.JSONCodec)
			defer badgerDB.Close()

			benchmarkDB(badgerDB)
		}()
	}
}

func BenchmarkBadgerDBWithLRUCache(b *testing.B) {
	for n := 0; n < b.N; n++ {
		func() {
			badgerDB := badgerdb.New(".badgerdb", codec.JSONCodec)
			defer badgerDB.Close()

			table := lru.NewLruTable(db.NewTable(badgerDB, "lru"), cacheLimit)
			benchmarkTable(table)
		}()
	}
}

func benchmarkDB(database db.DB) {
	key := "testKey"

	vals := make([]testutil.TestStruct, benchmarkWrites)

	for i := 0; i < benchmarkWrites; i++ {
		newKey := key + string(i)
		vals[i] = testutil.RandomTestStruct()
		Expect(database.Insert(newKey, vals[i])).NotTo(HaveOccurred())
	}

	for i := 0; i < benchmarkReads; i++ {
		queryIndex := rand.Intn(benchmarkWrites)
		queryKey := key + string(queryIndex)
		val := testutil.TestStruct{D: []byte{}}
		err := database.Get(queryKey, &val)
		Expect(err).NotTo(HaveOccurred())
		Expect(reflect.DeepEqual(val, vals[queryIndex])).Should(BeTrue())
	}
}

func benchmarkTable(table db.Table) {
	key := "testKey"

	vals := make([]testutil.TestStruct, benchmarkWrites)

	for i := 0; i < benchmarkWrites; i++ {
		newKey := key + string(i)
		vals[i] = testutil.RandomTestStruct()
		Expect(table.Insert(newKey, vals[i])).NotTo(HaveOccurred())
	}

	for i := 0; i < benchmarkReads; i++ {
		queryIndex := rand.Intn(benchmarkWrites)
		queryKey := key + string(queryIndex)
		val := testutil.TestStruct{D: []byte{}}
		err := table.Get(queryKey, &val)
		Expect(err).NotTo(HaveOccurred())
		Expect(reflect.DeepEqual(val, vals[queryIndex])).Should(BeTrue())
	}
}

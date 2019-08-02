package kv_test

import (
	"math/rand"
	"os/exec"
	"testing"

	"github.com/dgraph-io/badger"
	bdb "github.com/renproject/kv/badgerdb"
	"github.com/renproject/kv/db"
	ldb "github.com/renproject/kv/leveldb"
	"github.com/syndtr/goleveldb/leveldb"

	. "github.com/onsi/gomega"
)

const (
	benchmarkReads  = 10
	benchmarkWrites = 100
)

func BenchmarkLevelDB(b *testing.B) {
	for n := 0; n < b.N; n++ {
		func() {
			leveldb := initLevelDB()
			defer closeLevelDB(leveldb)

			lDB := ldb.New(leveldb)
			benchmarkDB(lDB)
		}()
	}
}

func BenchmarkBadgerDB(b *testing.B) {
	for n := 0; n < b.N; n++ {
		func() {
			badgerDB := initBadgerDB()
			defer closeBadgerDB(badgerDB)

			bDB := bdb.New(badgerDB)
			benchmarkDB(bDB)
		}()

	}
}

func benchmarkDB(database db.DB) {
	key := "testKey"

	for i := 0; i < benchmarkWrites; i++ {
		newKey := key + string(i)
		value := randBytes()
		Expect(database.Insert(newKey, value)).NotTo(HaveOccurred())
	}

	for i := 0; i < benchmarkReads; i++ {
		queryKey := key + string(rand.Intn(benchmarkWrites))
		_, err := database.Get(queryKey)
		Expect(err).NotTo(HaveOccurred())
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

package lru_test

import (
	"fmt"
	"math/rand"
	"os/exec"
	"reflect"
	"testing"
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/kv/cache/lru"

	"github.com/dgraph-io/badger"
	"github.com/renproject/kv/badgerdb"
	"github.com/renproject/kv/codec"
	"github.com/renproject/kv/db"
	"github.com/renproject/kv/testutil"
)

var codecs = []db.Codec{
	codec.JsonCodec,
	codec.GobCodec,
}

const (
	benchmarkReads  = 1000
	benchmarkWrites = 10
)

var _ = Describe("im-memory LRU implementation of the db", func() {
	for i := range codecs {
		codec := codecs[i]

		Context("when creating table", func() {
			It("should be able create a new table or getting existing ones", func() {
				tableTest := func(name string) bool {
					badgerDB := initDB()
					defer closeDB(badgerDB)

					lruDB := New(badgerdb.New(badgerDB), 100)

					table, err := lruDB.NewTable(name, codec)
					Expect(err).NotTo(HaveOccurred())
					Expect(table).ShouldNot(BeNil())

					tableByName, err := lruDB.Table(name)
					Expect(err).NotTo(HaveOccurred())
					Expect(tableByName).ShouldNot(BeNil())

					return true
				}

				Expect(quick.Check(tableTest, nil)).NotTo(HaveOccurred())
			})

			It("should be able to read and write values to the db", func() {
				readAndWrite := func(name string, key string, value testutil.TestStruct) bool {
					badgerDB := initDB()
					defer closeDB(badgerDB)

					lruDB := New(badgerdb.New(badgerDB), 100)
					table, err := lruDB.NewTable(name, codec)
					Expect(err).NotTo(HaveOccurred())
					Expect(table).ShouldNot(BeNil())

					val := testutil.TestStruct{D: []byte{}}
					err = lruDB.Get(name, key, &val)
					Expect(err).Should(Equal(db.ErrKeyNotFound))

					// Should be able to read the value after inserting.
					Expect(lruDB.Insert(name, key, value)).NotTo(HaveOccurred())
					err = lruDB.Get(name, key, &val)
					Expect(err).NotTo(HaveOccurred())
					Expect(reflect.DeepEqual(val, value)).Should(BeTrue())

					// Expect no value exists after deleting the value.
					Expect(lruDB.Delete(name, key)).NotTo(HaveOccurred())
					err = lruDB.Get(name, key, &val)
					Expect(err).Should(Equal(db.ErrKeyNotFound))

					return true
				}

				Expect(quick.Check(readAndWrite, nil)).NotTo(HaveOccurred())
			})

			It("should be able to iterable through the db using the iterator", func() {
				iteration := func(name string, values []testutil.TestStruct) bool {
					badgerDB := initDB()
					defer closeDB(badgerDB)

					lruDB := New(badgerdb.New(badgerDB), 100)
					table, err := lruDB.NewTable(name, codec)
					Expect(err).NotTo(HaveOccurred())
					Expect(table).ShouldNot(BeNil())

					// Insert all values and make a map for validation.
					allValues := map[string]testutil.TestStruct{}
					for i, value := range values {
						key := fmt.Sprintf("%v", i)
						Expect(lruDB.Insert(name, key, value)).NotTo(HaveOccurred())
						allValues[key] = value
					}

					size, err := lruDB.Size(name)
					Expect(err).NotTo(HaveOccurred())
					Expect(size).Should(Equal(len(values)))

					// Expect iterator gives us all the key-value pairs we insert.
					iter, err := lruDB.Iterator(name)
					Expect(err).NotTo(HaveOccurred())
					Expect(iter)

					for iter.Next() {
						key, err := iter.Key()
						Expect(err).NotTo(HaveOccurred())
						value := testutil.TestStruct{D: []byte{}}
						err = iter.Value(&value)
						Expect(err).NotTo(HaveOccurred())

						stored, ok := allValues[key]
						Expect(ok).Should(BeTrue())
						Expect(reflect.DeepEqual(value, stored)).Should(BeTrue())
						delete(allValues, key)
					}
					return len(allValues) == 0
				}

				Expect(quick.Check(iteration, nil)).NotTo(HaveOccurred())
			})
		})

		Context("when doing operations on a non-exist table", func() {
			It("should return ErrTableNotFound", func() {
				test := func(name string, key string, value testutil.TestStruct) bool {
					badgerDB := initDB()
					defer closeDB(badgerDB)

					lruDB := New(badgerdb.New(badgerDB), 100)

					// Retrieve table
					_, err := lruDB.Table(name)
					Expect(err).Should(Equal(db.ErrTableNotFound))

					// Insert new key-value pair
					err = lruDB.Insert(name, key, value)
					Expect(err).Should(Equal(db.ErrTableNotFound))

					// Retrieve value
					var val testutil.TestStruct
					err = lruDB.Get(name, key, &val)
					Expect(err).Should(Equal(db.ErrTableNotFound))

					// Delete data
					err = lruDB.Delete(name, key)
					Expect(err).Should(Equal(db.ErrTableNotFound))

					// Get size
					_, err = lruDB.Size(name)
					Expect(err).Should(Equal(db.ErrTableNotFound))

					// Get the iterator
					_, err = lruDB.Iterator(name)
					Expect(err).Should(Equal(db.ErrTableNotFound))

					return true
				}

				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})

		Context("when trying to create a table which already exist", func() {
			It("should return ErrTableAlreadyExists error", func() {
				test := func(name string) bool {
					badgerDB := initDB()
					defer closeDB(badgerDB)

					lruDB := New(badgerdb.New(badgerDB), 100)
					_, err := lruDB.NewTable(name, codec)
					Expect(err).NotTo(HaveOccurred())

					_, err = lruDB.NewTable(name, codec)
					Expect(err).Should(Equal(db.ErrTableAlreadyExists))

					return true
				}

				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})

		Context("when doing operations with empty keys", func() {
			It("should return ErrEmptyKey error", func() {
				test := func(name string, value testutil.TestStruct) bool {
					badgerDB := initDB()
					defer closeDB(badgerDB)

					lruDB := New(badgerdb.New(badgerDB), 100)
					_, err := lruDB.NewTable(name, codec)
					Expect(err).NotTo(HaveOccurred())

					Expect(lruDB.Insert(name, "", value)).Should(Equal(db.ErrEmptyKey))
					Expect(lruDB.Get(name, "", value)).Should(Equal(db.ErrEmptyKey))
					Expect(lruDB.Delete(name, "")).Should(Equal(db.ErrEmptyKey))

					return true
				}

				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})
	}
})

func BenchmarkLRU(b *testing.B) {
	for n := 0; n < b.N; n++ {
		func() {
			badgerDB := initDB()
			defer closeDB(badgerDB)

			lruDB := New(badgerdb.New(badgerDB), benchmarkWrites)
			benchmarkDB(lruDB)
		}()
	}
}

func BenchmarkBadgerDB(b *testing.B) {
	for n := 0; n < b.N; n++ {
		func() {
			badgerDB := initDB()
			defer closeDB(badgerDB)

			bDB := badgerdb.New(badgerDB)
			benchmarkDB(bDB)
		}()

	}
}

func benchmarkDB(database db.DB) {
	name := "testDB"
	key := "testKey"

	table, err := database.NewTable(name, codec.GobCodec)
	Expect(err).NotTo(HaveOccurred())
	Expect(table).ShouldNot(BeNil())

	for i := 0; i < benchmarkWrites; i++ {
		newKey := key + string(i)
		value := testutil.RandomTestStruct()
		Expect(database.Insert(name, newKey, value)).NotTo(HaveOccurred())
	}

	for i := 0; i < benchmarkReads; i++ {
		queryKey := key + string(rand.Intn(benchmarkWrites))
		val := testutil.TestStruct{D: []byte{}}
		err = database.Get(name, queryKey, &val)
		Expect(err).NotTo(HaveOccurred())
	}
}

func initDB() *badger.DB {
	Expect(exec.Command("mkdir", "-p", ".badgerdb").Run()).NotTo(HaveOccurred())
	opts := badger.DefaultOptions("./.badgerdb")
	opts.Dir = "./.badgerdb"
	opts.ValueDir = "./.badgerdb"
	db, err := badger.Open(opts.WithLogger(nil))
	Expect(err).NotTo(HaveOccurred())
	return db
}

func closeDB(db *badger.DB) {
	Expect(db.Close()).NotTo(HaveOccurred())
	Expect(exec.Command("rm", "-rf", "./.badgerdb").Run()).NotTo(HaveOccurred())
}

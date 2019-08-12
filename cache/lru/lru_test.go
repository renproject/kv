package lru_test

import (
	"fmt"
	"os/exec"
	"reflect"
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

var _ = Describe("in-memory LRU cache", func() {
	initDB := func() *badger.DB {
		Expect(exec.Command("mkdir", "-p", ".badgerdb").Run()).NotTo(HaveOccurred())
		opts := badger.DefaultOptions("./.badgerdb")
		opts.Dir = "./.badgerdb"
		opts.ValueDir = "./.badgerdb"
		db, err := badger.Open(opts.WithLogger(nil))
		Expect(err).NotTo(HaveOccurred())
		return db
	}

	closeDB := func(db *badger.DB) {
		Expect(db.Close()).NotTo(HaveOccurred())
		Expect(exec.Command("rm", "-rf", "./.badgerdb").Run()).NotTo(HaveOccurred())
	}

	for i := range codecs {
		codec := codecs[i]

		Context("when creating table", func() {
			It("should be able to read and write values to the db", func() {
				readAndWrite := func(name string, key string, value testutil.TestStruct) bool {
					badgerDB := initDB()
					defer closeDB(badgerDB)

					lruDB := New(badgerdb.New(badgerDB, codec), 100)

					// Make sure the key is not nil
					if key == "" {
						return true
					}
					val := testutil.TestStruct{D: []byte{}}
					err := lruDB.Get(name, key, &val)
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

					lruDB := New(badgerdb.New(badgerDB, codec), 100)

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
					Expect(iter).ShouldNot(BeNil())

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

		Context("when doing operations with empty keys", func() {
			It("should return ErrEmptyKey error", func() {
				test := func(name string, value testutil.TestStruct) bool {
					badgerDB := initDB()
					defer closeDB(badgerDB)

					lruDB := New(badgerdb.New(badgerDB, codec), 100)

					Expect(lruDB.Insert(name, "", value)).Should(Equal(db.ErrEmptyKey))
					Expect(lruDB.Get(name, "", value)).Should(Equal(db.ErrEmptyKey))

					return true
				}

				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})
	}
})

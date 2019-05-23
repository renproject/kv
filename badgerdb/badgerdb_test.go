package badgerdb_test

import (
	"bytes"
	"fmt"
	"os/exec"
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/kv/badgerdb"

	"github.com/dgraph-io/badger"
	"github.com/renproject/kv/db"
)

var _ = Describe("BadgerDB implementation of key-value Store", func() {

	initDB := func() *badger.DB {
		Expect(exec.Command("mkdir", "-p", ".badgerdb").Run()).NotTo(HaveOccurred())
		opts := badger.DefaultOptions
		opts.Dir = "./.badgerdb"
		opts.ValueDir = "./.badgerdb"
		db, err := badger.Open(opts)
		Expect(err).NotTo(HaveOccurred())
		return db
	}

	closeDB := func(db *badger.DB) {
		Expect(db.Close()).NotTo(HaveOccurred())
		Expect(exec.Command("rm", "-rf", "./.badgerdb").Run()).NotTo(HaveOccurred())
	}

	Context("when reading and writing", func() {
		It("should be able read and write value", func() {
			badgerdb := initDB()
			defer closeDB(badgerdb)

			readAndWrite := func(key string, value []byte) bool {
				ldb := New(badgerdb)
				if key == "" {
					return true
				}

				// Expect not value exists in the db with the given key.
				_, err := ldb.Get(key)
				Expect(err).Should(Equal(db.ErrNotFound))

				// Should be able to read the value after inserting.
				Expect(ldb.Insert(key, value)).NotTo(HaveOccurred())
				data, err := ldb.Get(key)
				Expect(err).NotTo(HaveOccurred())
				Expect(bytes.Compare(data, value)).Should(BeZero())

				// Expect no value exists after deleting the value.
				Expect(ldb.Delete(key)).NotTo(HaveOccurred())
				_, err = ldb.Get(key)
				return err == db.ErrNotFound
			}
			Expect(quick.Check(readAndWrite, nil)).NotTo(HaveOccurred())
		})

		It("should be able to iterable through the db using the iterator", func() {
			badgerdb := initDB()
			defer closeDB(badgerdb)

			iteration := func(values [][]byte) bool {
				ldb := New(badgerdb)

				// Insert all values and make a map for validation.
				allValues := map[string][]byte{}
				for i, value := range values {
					key := fmt.Sprintf("%v", i)
					Expect(ldb.Insert(key, value)).NotTo(HaveOccurred())
					allValues[key] = value
				}

				// Expect db size to the number of values we insert.
				size, err := ldb.Size()
				Expect(err).NotTo(HaveOccurred())
				Expect(size).Should(Equal(len(values)))

				// Expect iterator gives us all the key-value pairs we insert.
				iter := ldb.Iterator()
				for iter.Next() {
					key, err := iter.Key()
					Expect(err).NotTo(HaveOccurred())
					value, err := iter.Value()
					Expect(err).NotTo(HaveOccurred())
					Expect(ldb.Delete(key)).NotTo(HaveOccurred())

					stored, ok := allValues[key]
					Expect(ok).Should(BeTrue())
					Expect(bytes.Compare(value, stored)).Should(BeZero())
					delete(allValues, key)
				}
				return len(allValues) == 0
			}

			Expect(quick.Check(iteration, nil)).NotTo(HaveOccurred())
		})

		It("should return error when trying to get key/value when the iterator doesn't have next value", func() {
			badgerdb := initDB()
			defer closeDB(badgerdb)

			iteration := func(key string, value []byte) bool {
				ldb := New(badgerdb)
				iter := ldb.Iterator()

				for iter.Next() {
				}

				_, err := iter.Key()
				Expect(err).Should(Equal(db.ErrIndexOutOfRange))
				_, err = iter.Value()
				Expect(err).Should(Equal(db.ErrIndexOutOfRange))
				return iter.Next() == false
			}

			Expect(quick.Check(iteration, nil)).NotTo(HaveOccurred())
		})

		It("should return error when trying to get key/value without calling next()", func() {
			badgerdb := initDB()
			defer closeDB(badgerdb)

			iteration := func(key string, value []byte) bool {
				ldb := New(badgerdb)
				iter := ldb.Iterator()

				_, err := iter.Key()
				Expect(err).Should(Equal(db.ErrIndexOutOfRange))
				_, err = iter.Value()
				Expect(err).Should(Equal(db.ErrIndexOutOfRange))
				return iter.Next() == false
			}

			Expect(quick.Check(iteration, nil)).NotTo(HaveOccurred())
		})
	})
})

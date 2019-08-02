package leveldb_test

import (
	"bytes"
	"fmt"
	"os/exec"
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/kv/leveldb"

	"github.com/renproject/kv/db"
	"github.com/syndtr/goleveldb/leveldb"
)

var _ = Describe("levelDB implementation of key-value Store", func() {

	initDB := func() *leveldb.DB {
		db, err := leveldb.OpenFile("./.leveldb", nil)
		Expect(err).NotTo(HaveOccurred())
		return db
	}

	closeDB := func(db *leveldb.DB) {
		Expect(db.Close()).NotTo(HaveOccurred())
		Expect(exec.Command("rm", "-rf", "./.leveldb").Run()).NotTo(HaveOccurred())
	}

	Context("when reading and writing", func() {
		It("should be able read and write value", func() {
			leveldb := initDB()
			defer closeDB(leveldb)

			readAndWrite := func(key string, value []byte) bool {
				ldb := New(leveldb)
				if key == ""{
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
			leveldb := initDB()
			defer closeDB(leveldb)

			iteration := func(values [][]byte) bool {
				ldb := New(leveldb)

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
			leveldb := initDB()
			defer closeDB(leveldb)

			iteration := func(key string, value []byte) bool {
				ldb := New(leveldb)
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

		It("should return error when trying to get key/value without calling Next()", func() {
			leveldb := initDB()
			defer closeDB(leveldb)

			iteration := func(key string, value []byte) bool {
				ldb := New(leveldb)
				iter := ldb.Iterator()

				_, err := iter.Key()
				Expect(err).Should(Equal(db.ErrIndexOutOfRange))
				_, err = iter.Value()
				Expect(err).Should(Equal(db.ErrIndexOutOfRange))
				return iter.Next() == false
			}

			Expect(quick.Check(iteration, nil)).NotTo(HaveOccurred())
		})

		It("should return ErrEmptyKey when trying to insert a value with empty key", func() {
			leveldb := initDB()
			defer closeDB(leveldb)

			iteration := func(value []byte) bool {
				ldb := New(leveldb)
				return ldb.Insert("", value) == db.ErrEmptyKey
			}

			Expect(quick.Check(iteration, nil)).NotTo(HaveOccurred())
		})
	})
})
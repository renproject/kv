package memdb_test

import (
	"bytes"
	"fmt"
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/kv/memdb"

	"github.com/renproject/kv/db"
)

var _ = Describe("im-memory implementation of the db", func() {
	Context("when reading and writing", func() {
		It("should be able read and write value", func() {
			readAndWrite := func(key string, value []byte) bool {
				memDB := New()

				// Expect not value exists in the db with the given key.
				_, err := memDB.Get(key)
				Expect(err).Should(Equal(db.ErrNotFound))

				// Should be able to read the value after inserting.
				Expect(memDB.Insert(key, value)).NotTo(HaveOccurred())
				data, err := memDB.Get(key)
				Expect(err).NotTo(HaveOccurred())
				Expect(bytes.Compare(data, value)).Should(BeZero())

				// Expect no value exists after deleting the value.
				Expect(memDB.Delete(key)).NotTo(HaveOccurred())
				_, err = memDB.Get(key)
				return err == db.ErrNotFound
			}

			Expect(quick.Check(readAndWrite, nil)).NotTo(HaveOccurred())
		})

		It("should be able to iterable through the db using the iterator", func() {
			iteration := func(values [][]byte) bool {
				memDB := New()

				// Insert all values and make a map for validation.
				allValues := map[string][]byte{}
				for i, value := range values {
					key := fmt.Sprintf("%v", i)
					Expect(memDB.Insert(key, value)).NotTo(HaveOccurred())
					allValues[key] = value
				}

				// Expect db size to the number of values we insert.
				size, err := memDB.Size()
				Expect(err).NotTo(HaveOccurred())
				Expect(size).Should(Equal(len(values)))

				// Expect iterator gives us all the key-value pairs we insert.
				iter := memDB.Iterator()
				for iter.Next() {
					key, err := iter.Key()
					Expect(err).NotTo(HaveOccurred())
					value, err := iter.Value()
					Expect(err).NotTo(HaveOccurred())

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
			iteration := func(key string, value []byte) bool {
				memDB := New()
				iter := memDB.Iterator()

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
			iteration := func(key string, value []byte) bool {
				memDB := New()
				iter := memDB.Iterator()

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

package ttl_test

import (
	"fmt"
	"reflect"
	"testing/quick"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/kv/cache/ttl"

	"github.com/renproject/kv/codec"
	"github.com/renproject/kv/db"
	"github.com/renproject/kv/memdb"
	"github.com/renproject/kv/testutil"
)

var codecs = []db.Codec{
	codec.JsonCodec,
	codec.GobCodec,
}

var _ = Describe("in-memory LRU cache", func() {
	for i := range codecs {
		codec := codecs[i]

		Context("when creating table", func() {
			It("should be able create a new table or getting existing ones", func() {
				tableTest := func(name string) bool {
					ttlDB, err := New(memdb.New(), time.Second, 100*time.Millisecond, codec)
					Expect(err).NotTo(HaveOccurred())

					table, err := ttlDB.NewTable(name, codec)
					Expect(err).NotTo(HaveOccurred())
					Expect(table).ShouldNot(BeNil())

					tableByName, err := ttlDB.Table(name)
					Expect(err).NotTo(HaveOccurred())
					Expect(tableByName).ShouldNot(BeNil())

					return true
				}

				Expect(quick.Check(tableTest, nil)).NotTo(HaveOccurred())
			})

			It("should be able to read and write values to the db", func() {
				readAndWrite := func(name string, key string, value testutil.TestStruct) bool {
					ttlDB, err := New(memdb.New(), 10*time.Second, 5*time.Second, codec)
					Expect(err).NotTo(HaveOccurred())
					table, err := ttlDB.NewTable(name, codec)
					Expect(err).NotTo(HaveOccurred())
					Expect(table).ShouldNot(BeNil())

					// Make sure the key is not nil
					if key == "" {
						return true
					}
					val := testutil.TestStruct{D: []byte{}}
					err = ttlDB.Get(name, key, &val)
					Expect(err).Should(Equal(db.ErrKeyNotFound))

					// Should be able to read the value after inserting.
					Expect(ttlDB.Insert(name, key, value)).NotTo(HaveOccurred())
					err = ttlDB.Get(name, key, &val)
					Expect(err).NotTo(HaveOccurred())
					Expect(reflect.DeepEqual(val, value)).Should(BeTrue())

					// Expect no value exists after deleting the value.
					Expect(ttlDB.Delete(name, key)).NotTo(HaveOccurred())
					err = ttlDB.Get(name, key, &val)
					Expect(err).Should(Equal(db.ErrKeyNotFound))

					return true
				}

				Expect(quick.Check(readAndWrite, nil)).NotTo(HaveOccurred())
			})

			It("should be able to iterate through the db using the iterator", func() {
				iteration := func(name string, values []testutil.TestStruct) bool {
					ttlDB, err := New(memdb.New(), time.Second, 100*time.Millisecond, codec)
					Expect(err).NotTo(HaveOccurred())
					table, err := ttlDB.NewTable(name, codec)
					Expect(err).NotTo(HaveOccurred())
					Expect(table).ShouldNot(BeNil())

					// Insert all values and make a map for validation.
					allValues := map[string]testutil.TestStruct{}
					for i, value := range values {
						key := fmt.Sprintf("%v", i)
						Expect(ttlDB.Insert(name, key, value)).NotTo(HaveOccurred())
						allValues[key] = value
					}

					size, err := ttlDB.Size(name)
					Expect(err).NotTo(HaveOccurred())
					Expect(size).Should(Equal(len(values)))

					// Expect iterator gives us all the key-value pairs we insert.
					iter, err := ttlDB.Iterator(name)
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

		Context("when doing operations on a non-exist table", func() {
			It("should return ErrTableNotFound", func() {
				test := func(name string, key string, value testutil.TestStruct) bool {
					ttlDB, err := New(memdb.New(), time.Second, 100*time.Millisecond, codec)
					Expect(err).NotTo(HaveOccurred())

					// Retrieve table
					_, err = ttlDB.Table(name)
					Expect(err).Should(Equal(db.ErrTableNotFound))

					// Make sure the key is not nil
					if key == "" {
						return true
					}

					// Insert new key-value pair
					err = ttlDB.Insert(name, key, value)
					Expect(err).Should(Equal(db.ErrTableNotFound))

					// Retrieve value
					var val testutil.TestStruct
					err = ttlDB.Get(name, key, &val)
					Expect(err).Should(Equal(db.ErrTableNotFound))

					// Delete data
					err = ttlDB.Delete(name, key)
					Expect(err).Should(Equal(db.ErrTableNotFound))

					// Get size
					_, err = ttlDB.Size(name)
					Expect(err).Should(Equal(db.ErrTableNotFound))

					// Get the iterator
					_, err = ttlDB.Iterator(name)
					Expect(err).Should(Equal(db.ErrTableNotFound))

					return true
				}

				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})

		Context("when trying to create a table which already exist", func() {
			It("should return ErrTableAlreadyExists error", func() {
				test := func(name string) bool {
					ttlDB, err := New(memdb.New(), time.Second, 100*time.Millisecond, codec)
					Expect(err).NotTo(HaveOccurred())
					_, err = ttlDB.NewTable(name, codec)
					Expect(err).NotTo(HaveOccurred())

					_, err = ttlDB.NewTable(name, codec)
					Expect(err).Should(Equal(db.ErrTableAlreadyExists))

					return true
				}

				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})

		Context("when doing operations with empty keys", func() {
			It("should return ErrEmptyKey error", func() {
				test := func(name string, value testutil.TestStruct) bool {
					ttlDB, err := New(memdb.New(), time.Second, 100*time.Millisecond, codec)
					Expect(err).NotTo(HaveOccurred())
					_, err = ttlDB.NewTable(name, codec)
					Expect(err).NotTo(HaveOccurred())

					Expect(ttlDB.Insert(name, "", value)).Should(Equal(db.ErrEmptyKey))
					Expect(ttlDB.Get(name, "", value)).Should(Equal(db.ErrEmptyKey))
					Expect(ttlDB.Delete(name, "")).Should(Equal(db.ErrEmptyKey))

					return true
				}

				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})

		Context("when reading and writing with data-expiration", func() {
			It("should be able return error if the data has expired", func() {
				readAndWrite := func(name, key string, value testutil.TestStruct) bool {
					if key == "" {
						return true
					}

					ttlDB, err := New(memdb.New(), 1*time.Second, 10*time.Millisecond, codec)
					Expect(err).NotTo(HaveOccurred())
					_, err = ttlDB.NewTable(name, codec)
					Expect(err).NotTo(HaveOccurred())

					var newValue testutil.TestStruct
					Expect(ttlDB.Get(name, key, &newValue)).Should(Equal(db.ErrKeyNotFound))
					Expect(ttlDB.Insert(name, key, &value)).NotTo(HaveOccurred())
					Expect(ttlDB.Get(name, key, &newValue)).NotTo(HaveOccurred())
					Expect(value.Equal(newValue)).Should(BeTrue())

					time.Sleep(1100 * time.Millisecond)
					Expect(ttlDB.Get(name, key, &newValue)).To(Equal(db.ErrKeyNotFound))

					return true
				}

				Expect(quick.Check(readAndWrite, nil)).NotTo(HaveOccurred())
			})

			It("should be able to prune the database automatically", func() {
				readAndWrite := func(name, key string, value testutil.TestStruct) bool {
					if key == "" {
						return true
					}

					ttlDB, err := New(memdb.New(), 1*time.Second, 10*time.Millisecond, codec)
					Expect(err).NotTo(HaveOccurred())
					_, err = ttlDB.NewTable(name, codec)
					Expect(err).NotTo(HaveOccurred())

					var newValue testutil.TestStruct
					Expect(ttlDB.Get(name, key, &newValue)).Should(Equal(db.ErrKeyNotFound))
					Expect(ttlDB.Insert(name, key, &value)).NotTo(HaveOccurred())
					Expect(ttlDB.Get(name, key, &newValue)).NotTo(HaveOccurred())
					Expect(value.Equal(newValue)).Should(BeTrue())
					size, err := ttlDB.Size(name)
					Expect(err).NotTo(HaveOccurred())
					Expect(size).To(Equal(1))

					time.Sleep(1100 * time.Millisecond)
					size, err = ttlDB.Size(name)
					Expect(err).NotTo(HaveOccurred())
					Expect(size).To(Equal(0))

					return true
				}

				Expect(quick.Check(readAndWrite, nil)).NotTo(HaveOccurred())
			})
		})
	}
})

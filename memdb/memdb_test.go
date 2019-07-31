package memdb_test

import (
	"fmt"
	"reflect"
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/kv/memdb"

	"github.com/renproject/kv/db"
	"github.com/renproject/kv/testutil"
)

var _ = Describe("im-memory implementation of the db", func() {

	for i := range codecs {
		codec := codecs[i]

		Context("when creating table", func() {
			It("should be able create a new table or getting existing ones", func() {
				tableTest := func(name string) bool {
					memdb := New()
					table, err := memdb.NewTable(name, codec)
					Expect(err).NotTo(HaveOccurred())
					Expect(table).ShouldNot(BeNil())

					tableByName, err := memdb.Table(name)
					Expect(err).NotTo(HaveOccurred())
					Expect(tableByName).ShouldNot(BeNil())

					return true
				}

				Expect(quick.Check(tableTest, nil)).NotTo(HaveOccurred())
			})

			It("should be able to iterable through the db using the iterator", func() {
				readAndWrite := func(name string, key string, value testutil.TestStruct) bool {
					memdb := New()
					table, err := memdb.NewTable(name, codec)
					Expect(err).NotTo(HaveOccurred())
					Expect(table).ShouldNot(BeNil())

					// Make sure the key is not nil
					if key == "" {
						return true
					}
					val := testutil.TestStruct{D:[]byte{}}
					err = memdb.Get(name, key, &val)
					Expect(err).Should(Equal(db.ErrNotFound))

					// Should be able to read the value after inserting.
					Expect(memdb.Insert(name, key, value)).NotTo(HaveOccurred())
					err = memdb.Get(name, key, &val)
					Expect(err).NotTo(HaveOccurred())
					Expect(reflect.DeepEqual(val, value)).Should(BeTrue())

					// Expect no value exists after deleting the value.
					Expect(memdb.Delete(name, key)).NotTo(HaveOccurred())
					err = memdb.Get(name, key, &val)
					Expect(err).Should(Equal(db.ErrNotFound))

					return true
				}

				Expect(quick.Check(readAndWrite, nil)).NotTo(HaveOccurred())
			})

			It("should be able to iterable through the db using the iterator", func() {
				iteration := func(name string, values []testutil.TestStruct) bool {
					memdb := New()
					table, err := memdb.NewTable(name, codec)
					Expect(err).NotTo(HaveOccurred())
					Expect(table).ShouldNot(BeNil())

					// Insert all values and make a map for validation.
					allValues := map[string]testutil.TestStruct{}
					for i, value := range values {
						key := fmt.Sprintf("%v", i)
						Expect(memdb.Insert(name, key, value)).NotTo(HaveOccurred())
						allValues[key] = value
					}

					size, err := memdb.Size(name)
					Expect(err).NotTo(HaveOccurred())
					Expect(size).Should(Equal(len(values)))

					// Expect iterator gives us all the key-value pairs we insert.
					iter, err:= memdb.Iterator(name)
					Expect(err).NotTo(HaveOccurred())
					Expect(iter)

					for iter.Next() {
						key, err := iter.Key()
						Expect(err).NotTo(HaveOccurred())
						value := testutil.TestStruct{D:[]byte{}}
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
				test := func(name string, key string ,value testutil.TestStruct) bool {
					memdb := New()

					// Retriev table
					_, err := memdb.Table(name)
					Expect(err).Should(Equal(db.ErrTableNotFound))

					// Insert new key-value pair
					err = memdb.Insert(name, key, value )
					Expect(err).Should(Equal(db.ErrTableNotFound))

					// Retrieve value
					var val testutil.TestStruct
					err = memdb.Get(name, key, &val)
					Expect(err).Should(Equal(db.ErrTableNotFound))

					// Delete data
					err = memdb.Delete(name, key )
					Expect(err).Should(Equal(db.ErrTableNotFound))

					// Get size
					_, err = memdb.Size(name)
					Expect(err).Should(Equal(db.ErrTableNotFound))

					// Get the iterator
					_, err = memdb.Iterator(name)
					Expect(err).Should(Equal(db.ErrTableNotFound))

					return true
				}

				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})

		Context("when trying to create a table which already exist", func() {
			It( "should return ErrTableAlreadyExists error", func() {
				test := func(name string) bool {
					memdb := New()
					_, err := memdb.NewTable(name, codec)
					Expect(err).NotTo(HaveOccurred())

					_, err = memdb.NewTable(name, codec)
					Expect(err).Should(Equal(db.ErrTableAlreadyExists))

					return true
				}

				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})
	}
})

package memdb_test

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/kv/memdb"

	"github.com/renproject/kv/db"
	"github.com/renproject/kv/testutil"
	"github.com/renproject/phi"
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
		})

		Context("when operating on a single table", func() {
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
					val := testutil.TestStruct{D: []byte{}}
					err = memdb.Get(name, key, &val)
					Expect(err).Should(Equal(db.ErrKeyNotFound))

					// Should be able to read the value after inserting.
					Expect(memdb.Insert(name, key, value)).NotTo(HaveOccurred())
					err = memdb.Get(name, key, &val)
					Expect(err).NotTo(HaveOccurred())
					Expect(reflect.DeepEqual(val, value)).Should(BeTrue())

					// Expect no value exists after deleting the value.
					Expect(memdb.Delete(name, key)).NotTo(HaveOccurred())
					err = memdb.Get(name, key, &val)
					Expect(err).Should(Equal(db.ErrKeyNotFound))

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
					iter, err := memdb.Iterator(name)
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

		Context("when doing operations on multiple tables within the same DB", func() {
			It("should work properly when doing reading and writing", func() {
				readAndWrite := func() bool {
					memdb := New()
					names := testutil.RandomNonDupStrings(20)
					testEntries := testutil.RandomTestStructGroups(len(names), rand.Intn(20))
					errs := make([]error, len(names))

					// Should be able to concurrently creating tables from the same DB.
					phi.ParForAll(names, func(i int) {
						_, err := memdb.NewTable(names[i], codec)
						errs[i] = err
					})
					Expect(testutil.CheckErrors(errs)).Should(BeNil())

					// Should be able to concurrently read and write data of different tables.
					phi.ParForAll(names, func(i int) {
						entries := testEntries[i]

						// Inserting all data entries
						for j, entry := range entries {
							err := memdb.Insert(names[i], fmt.Sprintf("%v", j), entry)
							if err != nil {
								errs[i] = err
								return
							}
						}

						// Check the size function returning the right size of the table.
						size, err := memdb.Size(names[i])
						if err != nil {
							errs[i] = err
							return
						}
						if size != len(entries) {
							errs[i] = fmt.Errorf("test failed, unexpected table size, expect = %v, got = %v", len(entries), size)
							return
						}
						// Retrieve all data entries
						for j, entry := range entries {
							storedEntry := testutil.TestStruct{D: []byte{}}
							err := memdb.Get(names[i], fmt.Sprintf("%v", j), &storedEntry)
							if err != nil {
								errs[i] = err
								return
							}
							if !reflect.DeepEqual(storedEntry, entry) {
								errs[i] = fmt.Errorf("fail to retrieve data from the table %v", names[i])
								return
							}
						}
					})
					Expect(testutil.CheckErrors(errs)).Should(BeNil())

					return true
				}

				Expect(quick.Check(readAndWrite, nil)).NotTo(HaveOccurred())
			})

			It("should working properly when iterating each table at the same time", func() {
				iteration := func() bool {
					memdb := New()
					names := testutil.RandomNonDupStrings(20)
					testEntries := testutil.RandomTestStructGroups(len(names), rand.Intn(20))
					errs := make([]error, len(names))

					// Should be able to concurrently creating tables from the same DB.
					phi.ParForAll(names, func(i int) {
						_, err := memdb.NewTable(names[i], codec)
						errs[i] = err
					})
					Expect(testutil.CheckErrors(errs)).Should(BeNil())

					// Should be able to concurrently iterating different tables.
					phi.ParForAll(names, func(i int) {
						entries := testEntries[i]

						// Inserting all data entries
						allValues := map[string]testutil.TestStruct{}
						for j, entry := range entries {
							key := fmt.Sprintf("%v", j)
							err := memdb.Insert(names[i], fmt.Sprintf("%v", j), entry)
							if err != nil {
								errs[i] = err
								return
							}
							allValues[key] = entry
						}

						// Expect iterator gives us all the key-value pairs we inserted.
						iter, err := memdb.Iterator(names[i])
						if err != nil {
							errs[i] = err
							return
						}

						for iter.Next() {
							key, err := iter.Key()
							if err != nil {
								errs[i] = err
								return
							}
							value := testutil.TestStruct{D: []byte{}}
							err = iter.Value(&value)
							if err != nil {
								errs[i] = err
								return
							}

							stored, ok := allValues[key]
							if err != nil {
								errs[i] = err
								return
							}
							if !ok {
								errs[i] = errors.New("test failed, iterator has new values inserted after the iterator been created ")
								return
							}
							if !reflect.DeepEqual(value, stored) {
								errs[i] = errors.New("test failed, stored value different are different")
								return
							}
							delete(allValues, key)
						}
					})
					Expect(testutil.CheckErrors(errs)).Should(BeNil())

					return true
				}

				Expect(quick.Check(iteration, nil)).NotTo(HaveOccurred())
			})
		})

		Context("when doing operations on a non-exist table", func() {
			It("should return ErrTableNotFound", func() {
				test := func(name string, key string, value testutil.TestStruct) bool {
					memdb := New()

					// Retrieve table
					_, err := memdb.Table(name)
					Expect(err).Should(Equal(db.ErrTableNotFound))

					// Insert new key-value pair
					err = memdb.Insert(name, key, value)
					Expect(err).Should(Equal(db.ErrTableNotFound))

					// Retrieve value
					var val testutil.TestStruct
					err = memdb.Get(name, key, &val)
					Expect(err).Should(Equal(db.ErrTableNotFound))

					// Delete data
					err = memdb.Delete(name, key)
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
			It("should return ErrTableAlreadyExists error", func() {
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

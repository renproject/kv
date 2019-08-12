package badgerdb_test

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/kv/badgerdb"

	"github.com/renproject/kv/db"
	"github.com/renproject/kv/testutil"
	"github.com/renproject/phi"
)

var _ = Describe("badger DB implementation of the db", func() {

	for i := range codecs {
		codec := codecs[i]

		Context("when operating on a single table", func() {
			It("should be able to iterable through the db using the iterator", func() {
				readAndWrite := func(name string, key string, value testutil.TestStruct) bool {
					badgerDB := New(bdb, codec)

					// Make sure the key is not nil
					if key == "" {
						return true
					}
					val := testutil.TestStruct{D: []byte{}}
					err := badgerDB.Get(name, key, &val)
					Expect(err).Should(Equal(db.ErrKeyNotFound))

					// Should be able to read the value after inserting.
					Expect(badgerDB.Insert(name, key, value)).NotTo(HaveOccurred())
					err = badgerDB.Get(name, key, &val)
					Expect(err).NotTo(HaveOccurred())
					Expect(reflect.DeepEqual(val, value)).Should(BeTrue())

					// Expect no value exists after deleting the value.
					Expect(badgerDB.Delete(name, key)).NotTo(HaveOccurred())
					err = badgerDB.Get(name, key, &val)
					Expect(err).Should(Equal(db.ErrKeyNotFound))

					return true
				}

				Expect(quick.Check(readAndWrite, nil)).NotTo(HaveOccurred())
			})

			It("should be able to iterable through the db using the iterator", func() {
				iteration := func(name string, values []testutil.TestStruct) bool {
					badgerDB := New(bdb, codec)

					// Insert all values and make a map for validation.
					allValues := map[string]testutil.TestStruct{}
					for i, value := range values {
						key := fmt.Sprintf("%v", i)
						Expect(badgerDB.Insert(name, key, value)).NotTo(HaveOccurred())
						allValues[key] = value
					}

					size, err := badgerDB.Size(name)
					Expect(err).NotTo(HaveOccurred())
					Expect(size).Should(Equal(len(values)))

					// Expect iterator gives us all the key-value pairs we insert.
					iter, err := badgerDB.Iterator(name)
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
						Expect(badgerDB.Delete(name, key)).Should(Succeed())
					}
					return len(allValues) == 0
				}

				Expect(quick.Check(iteration, nil)).NotTo(HaveOccurred())
			})
		})

		Context("when doing operations on multiple tables within the same DB", func() {
			It("should work properly when doing reading and writing", func() {
				readAndWrite := func() bool {
					badgerDB := New(bdb, codec)
					names := testutil.RandomNonDupStrings(20)
					testEntries := testutil.RandomTestStructGroups(len(names), rand.Intn(20))
					errs := make([]error, len(names))

					// Should be able to concurrently read and write data of different tables.
					phi.ParForAll(names, func(i int) {
						entries := testEntries[i]

						// Inserting all data entries
						for j, entry := range entries {
							err := badgerDB.Insert(names[i], fmt.Sprintf("%v", j), entry)
							if err != nil {
								errs[i] = err
								return
							}
						}

						// Check the size function returning the right size of the table.
						size, err := badgerDB.Size(names[i])
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
							err := badgerDB.Get(names[i], fmt.Sprintf("%v", j), &storedEntry)
							if err != nil {
								errs[i] = err
								return
							}
							if !reflect.DeepEqual(storedEntry, entry) {
								errs[i] = fmt.Errorf("fail to retrieve data from the table %v", names[i])
								return
							}
							Expect(badgerDB.Delete(names[i], fmt.Sprintf("%v", j))).Should(Succeed())
						}
					})
					Expect(testutil.CheckErrors(errs)).Should(BeNil())

					return true
				}

				Expect(quick.Check(readAndWrite, nil)).NotTo(HaveOccurred())
			})

			It("should working properly when iterating each table at the same time", func() {
				iteration := func() bool {
					badgerDB := New(bdb, codec)
					names := testutil.RandomNonDupStrings(20)
					testEntries := testutil.RandomTestStructGroups(len(names), rand.Intn(20))
					errs := make([]error, len(names))

					// Should be able to concurrently iterating different tables.
					phi.ParForAll(names, func(i int) {
						entries := testEntries[i]

						// Inserting all data entries
						allValues := map[string]testutil.TestStruct{}
						for j, entry := range entries {
							key := fmt.Sprintf("%v", j)
							err := badgerDB.Insert(names[i], fmt.Sprintf("%v", j), entry)
							if err != nil {
								errs[i] = err
								return
							}
							allValues[key] = entry
						}

						// Expect iterator gives us all the key-value pairs we inserted.
						iter, err := badgerDB.Iterator(names[i])
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
							Expect(badgerDB.Delete(names[i], key)).Should(Succeed())
						}
					})
					Expect(testutil.CheckErrors(errs)).Should(BeNil())

					return true
				}

				Expect(quick.Check(iteration, nil)).NotTo(HaveOccurred())
			})
		})
	}
})

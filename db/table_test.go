package db_test

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/kv/db"

	"github.com/renproject/kv/testutil"
	"github.com/renproject/phi"
)

var _ = Describe("table", func() {
	for i := range testutil.Codecs {
		for j := range testutil.DbInitalizer {
			codec := testutil.Codecs[i]
			initializer := testutil.DbInitalizer[j]

			Context("concurrent read and write on different tables", func() {
				It("should work as expected without affecting other tables.", func() {
					db := initializer(codec)
					defer db.Close()

					test := func() bool {
						names := testutil.RandomNonDupStrings(20)
						testEntries := testutil.RandomTestStructGroups(len(names), rand.Intn(20))
						errs := make([]error, len(names))

						// Should be able to concurrently read and write data of different tables.
						phi.ParForAll(names, func(i int) {
							table := NewTable(db, names[i])
							entries := testEntries[i]

							errs[i] = func() error {
								// Inserting all data entries
								for j, entry := range entries {
									err := table.Insert(fmt.Sprintf("%v", j), entry)
									if err != nil {
										return err
									}
								}

								// Check the size function returning the right size of the Table.
								size, err := table.Size()
								if err != nil {
									return err

								}
								if size != len(entries) {
									return fmt.Errorf("test failed, unexpected Table size, expect = %v, got = %v", len(entries), size)
								}

								// Retrieve all data entries
								for j, entry := range entries {
									storedEntry := testutil.TestStruct{D: []byte{}}
									err := table.Get(fmt.Sprintf("%v", j), &storedEntry)
									if err != nil {
										return err
									}
									if !reflect.DeepEqual(storedEntry, entry) {
										return fmt.Errorf("fail to retrieve data from the Table %v", names[i])
									}
									Expect(table.Delete(fmt.Sprintf("%v", j))).Should(Succeed())
								}
								return nil
							}()

						})

						Expect(testutil.CheckErrors(errs)).Should(BeNil())

						size, err := db.Size("")
						Expect(err).NotTo(HaveOccurred())
						Expect(size).Should(BeZero())
						return true
					}

					Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
				})
			})

			It("should working properly when iterating each Table at the same time", func() {
				db := initializer(codec)
				defer db.Close()

				iteration := func() bool {
					names := testutil.RandomNonDupStrings(20)
					testEntries := testutil.RandomTestStructGroups(len(names), rand.Intn(20))
					errs := make([]error, len(names))

					// Should be able to concurrently iterating different tables.
					phi.ParForAll(names, func(i int) {
						table := NewTable(db, names[i])
						entries := testEntries[i]

						errs[i] = func() error {
							// Inserting all data entries
							allValues := map[string]testutil.TestStruct{}
							for j, entry := range entries {
								key := fmt.Sprintf("%v", j)
								err := table.Insert(key, entry)
								if err != nil {
									return err
								}
								allValues[key] = entry
							}

							// Expect iterator gives us all the key-value pairs we inserted.
							iter := table.Iterator()
							defer iter.Close()
							for iter.Next() {
								key, err := iter.Key()
								if err != nil {
									return err
								}
								value := testutil.TestStruct{D: []byte{}}
								err = iter.Value(&value)
								if err != nil {
									return err
								}

								stored, ok := allValues[key]
								if err != nil {
									return err
								}
								if !ok {
									return errors.New("test failed, iterator has new values inserted after the iterator been created ")
								}
								if !reflect.DeepEqual(value, stored) {
									return errors.New("test failed, stored value different are different")
								}
								delete(allValues, key)
								Expect(table.Delete(key)).Should(Succeed())
							}
							return nil
						}()

					})

					Expect(testutil.CheckErrors(errs)).Should(BeNil())

					// Expect nothing left in the DB
					size, err := db.Size("")
					Expect(err).NotTo(HaveOccurred())
					Expect(size).Should(BeZero())
					return true
				}

				Expect(quick.Check(iteration, nil)).NotTo(HaveOccurred())
			})
		}
	}
})

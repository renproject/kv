package lru_test

import (
	"fmt"
	"reflect"
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/kv/cache/lru"

	"github.com/renproject/kv/db"
	"github.com/renproject/kv/testutil"
)

var _ = Describe("lru cache table wrapper", func() {
	for i := range testutil.Codecs {
		for j := range testutil.DbInitalizer {
			codec := testutil.Codecs[i]
			initializer := testutil.DbInitalizer[j]

			Context("when creating a wrapped table.", func() {
				It("should be able to read and write", func() {
					database := initializer(codec)
					defer database.Close()

					readAndWrite := func(name string, key string, value testutil.TestStruct) bool {
						table := NewLruTable(db.NewTable(database, name), 10)

						// ignore test cases with empty key in positive tests.
						if key == "" {
							return true
						}

						// Expect no value exists in the db with the given key.
						// << Since gob will parse empty bytes as nil slice and reflect.DeepEqual returns false
						// << when comparing the, so we need to initialize D to be a non-nil slice.
						val := testutil.TestStruct{D: []byte{}}
						err := table.Get(key, &val)
						Expect(err).Should(Equal(db.ErrKeyNotFound))

						// Should be able to read the value after inserting.
						Expect(table.Insert(key, value)).NotTo(HaveOccurred())
						err = table.Get(key, &val)
						Expect(err).NotTo(HaveOccurred())
						Expect(reflect.DeepEqual(val, value)).Should(BeTrue())

						// Expect no value exists after deleting the value.
						Expect(table.Delete(key)).NotTo(HaveOccurred())
						err = table.Get(key, &val)
						Expect(err).Should(Equal(db.ErrKeyNotFound))

						return true
					}

					Expect(quick.Check(readAndWrite, nil)).NotTo(HaveOccurred())
				})

				It("should be able to iterate through the db using the iterator", func() {
					database := initializer(codec)
					defer database.Close()

					iteration := func(name string, values []testutil.TestStruct) bool {
						table := NewLruTable(db.NewTable(database, name), 10)

						// Insert all values and make a map for validation.
						allValues := map[string]testutil.TestStruct{}
						for i, value := range values {
							key := fmt.Sprintf("%v", i)
							Expect(table.Insert(key, value)).NotTo(HaveOccurred())
							allValues[key] = value
						}

						// Expect db size to be the number of values we insert.
						size, err := table.Size()
						Expect(err).NotTo(HaveOccurred())
						Expect(size).Should(Equal(len(values)))

						// Expect iterator gives us all the key-value pairs we insert.
						iter := table.Iterator()
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
							Expect(table.Delete(key)).Should(Succeed())
						}
						return len(allValues) == 0
					}

					Expect(quick.Check(iteration, nil)).NotTo(HaveOccurred())
				})
			})
		}
	}
})

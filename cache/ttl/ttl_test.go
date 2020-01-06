package ttl_test

import (
	"context"
	"fmt"
	"reflect"
	"testing/quick"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/kv/cache/ttl"

	"github.com/renproject/kv/db"
	"github.com/renproject/kv/testutil"
)

var _ = Describe("TTL cache", func() {

	readAndWrite := func(table db.Table, key string, value testutil.TestStruct) bool {
		// Make sure the key is not nil
		if key == "" {
			return true
		}

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

	iteration := func(table db.Table, values []testutil.TestStruct) bool {
		// Insert all values and make a map for validation.
		allValues := map[string]testutil.TestStruct{}
		for i, value := range values {
			key := fmt.Sprintf("%v", i)
			Expect(table.Insert(key, value)).NotTo(HaveOccurred())
			allValues[key] = value
		}

		size, err := table.Size()
		Expect(err).NotTo(HaveOccurred())
		Expect(size).Should(Equal(len(values)))

		// Expect iterator gives us all the key-value pairs we insert.
		iter := table.Iterator()
		Expect(iter).ShouldNot(BeNil())
		defer iter.Close()

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

	cleanTable := func(table db.Table) {
		iter := table.Iterator()
		defer iter.Close()

		for iter.Next() {
			key, err := iter.Key()
			Expect(err).NotTo(HaveOccurred())
			Expect(table.Delete(key)).NotTo(HaveOccurred())
		}
	}

	for i := range testutil.Codecs {
		for j := range testutil.DbInitalizer {
			codec := testutil.Codecs[i]
			initializer := testutil.DbInitalizer[j]

			Context("when creating table", func() {
				It("should be able to read and write values to the db", func() {
					database := initializer(codec)
					defer database.Close()

					test := func(name string, key string, value testutil.TestStruct) bool {
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						table := New(ctx, database, name, 5*time.Second)
						defer cleanTable(table)
						return readAndWrite(table, key, value)
					}

					Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
				})

				It("should be able to iterate through the db using the iterator", func() {
					database := initializer(codec)
					defer database.Close()

					test := func(name string, key string, values []testutil.TestStruct) bool {
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						table := New(ctx, database, name, 5*time.Second)
						defer cleanTable(table)
						return iteration(table, values)
					}

					Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
				})
			})

			Context("when doing operations with empty keys", func() {
				It("should return ErrEmptyKey error", func() {
					database := initializer(codec)
					defer database.Close()

					test := func(name string, value testutil.TestStruct) bool {
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						table := New(ctx, database, name, 5*time.Second)

						Expect(table.Insert("", value)).Should(Equal(db.ErrEmptyKey))
						Expect(table.Get("", value)).Should(Equal(db.ErrEmptyKey))
						Expect(table.Delete("")).Should(Equal(db.ErrEmptyKey))

						return true
					}

					Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
				})
			})

			Context("when creating multiple ttl table with same underlying db", func() {
				It("should not affect each other", func() {
					database := initializer(codec)
					defer database.Close()

					tableNames := map[string]struct{}{}

					test := func(name string, key string, value testutil.TestStruct, values []testutil.TestStruct) bool {
						// Need to make sure all tables have different names
						if _, ok := tableNames[name]; ok {
							return true
						}
						tableNames[name] = struct{}{}

						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						table := New(ctx, database, name, 5*time.Second)
						Expect(readAndWrite(table, key, value)).Should(BeTrue())
						Expect(iteration(table, values)).Should(BeTrue())

						return true
					}

					Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
				})
			})

			Context("when reading and writing with data-expiration", func() {
				It("should return an error if the data has expired", func() {
					database := initializer(codec)
					defer database.Close()

					test := func(name, key string, value testutil.TestStruct) bool {
						if key == "" {
							return true
						}
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						table := New(ctx, database, name, 100*time.Millisecond)
						newValue := testutil.TestStruct{D: []byte{}}
						Expect(table.Get(key, &newValue)).Should(Equal(db.ErrKeyNotFound))
						Expect(table.Insert(key, &value)).NotTo(HaveOccurred())
						Expect(table.Get(key, &newValue)).NotTo(HaveOccurred())
						Expect(reflect.DeepEqual(value, newValue)).Should(BeTrue())

						Eventually(func() error {
							return table.Get(key, &newValue)
						}, time.Second, 100*time.Millisecond).Should(Equal(db.ErrKeyNotFound))

						return true
					}

					Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
				})

				It("should not prune for at least prune interval duration", func() {
					database := initializer(codec)
					defer database.Close()

					readAndWrite := func(name, key string, value testutil.TestStruct) bool {
						if key == "" {
							return true
						}

						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()

						table := New(ctx, database, name, 50*time.Millisecond)
						Expect(table.Insert(key, &value)).NotTo(HaveOccurred())

						time.Sleep(40 * time.Millisecond)

						size, err := table.Size()
						Expect(err).NotTo(HaveOccurred())
						Expect(size).To(Equal(1))

						Expect(table.Delete(key)).NotTo(HaveOccurred())

						return true
					}

					Expect(quick.Check(readAndWrite, nil)).NotTo(HaveOccurred())
				})

				It("should eventually prune the data", func() {
					database := initializer(codec)
					defer database.Close()

					readAndWrite := func(name, key string, value testutil.TestStruct) bool {
						if key == "" {
							return true
						}

						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()

						table := New(ctx, database, name, 50*time.Millisecond)
						Expect(table.Insert(key, &value)).NotTo(HaveOccurred())

						Eventually(func() int {
							size, err := table.Size()
							Expect(err).NotTo(HaveOccurred())
							return size
						}, time.Second, 50*time.Millisecond).Should(Equal(0))

						return true
					}

					Expect(quick.Check(readAndWrite, nil)).NotTo(HaveOccurred())
				})

				It("should not prune if the same key is added again before the interval expires", func() {
					database := initializer(codec)
					defer database.Close()

					key := "key"
					value := testutil.RandomTestStruct()

					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()

					table := New(ctx, database, "name", 100*time.Millisecond)
					Expect(table.Insert(key, &value)).NotTo(HaveOccurred())

					for i := 0; i < 100; i++ {
						time.Sleep(30 * time.Millisecond)

						size, err := table.Size()
						Expect(err).ToNot(HaveOccurred())
						Expect(size).To(Equal(1))

						Expect(table.Insert(key, &value)).NotTo(HaveOccurred())
					}
				})
			})
		}
	}
})

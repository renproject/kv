package memdb_test

import (
	"errors"
	"fmt"
	"reflect"
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/kv/memdb"
	"github.com/renproject/phi"

	"github.com/renproject/kv/codec"
	"github.com/renproject/kv/db"
	"github.com/renproject/kv/testutil"
)

var codecs = []db.Codec{
	codec.JsonCodec,
	codec.GobCodec,
}

var _ = Describe("im-memory implementation of the table", func() {
	for i := range codecs {
		codec := codecs[i]

		Context(fmt.Sprintf("when reading and writing using %v codec", codec), func() {
			It("should be able read and write value", func() {
				readAndWrite := func(key string, value testutil.TestStruct) bool {
					table := NewTable(codec)
					if key == "" {
						return true
					}

					// Expect no value exists in the db with the given key.
					// << Since gob will parse empty bytes to nil slice. reflect.DeepEqual will return false.
					// << So we need to initialize D to be a non-nil slice.
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

			It("should be able to iterable through the db using the iterator", func() {
				iteration := func(values []testutil.TestStruct) bool {
					table := NewTable(codec)

					// Insert all values and make a map for validation.
					allValues := map[string]testutil.TestStruct{}
					for i, value := range values {
						key := fmt.Sprintf("%v", i)
						Expect(table.Insert(key, value)).NotTo(HaveOccurred())
						allValues[key] = value
					}

					// Expect db size to the number of values we insert.
					size, err := table.Size()
					Expect(err).NotTo(HaveOccurred())
					Expect(size).Should(Equal(len(values)))

					// Expect iterator gives us all the key-value pairs we insert.
					iter, err := table.Iterator()
					Expect(err).NotTo(HaveOccurred())
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

			FIt("should return a iterator which only has the view of the table at the time been created", func() {
				iteration := func(values []testutil.TestStruct) bool {
					table := NewTable(codec)

					// Insert all values and make a map for validation.
					allValues := map[string]testutil.TestStruct{}
					for i, value := range values {
						key := fmt.Sprintf("%v", i)
						Expect(table.Insert(key, value)).NotTo(HaveOccurred())
						allValues[key] = value
					}

					// Expect iterator gives us all the key-value pairs we insert iter been created.
					iter, err := table.Iterator()
					Expect(err).NotTo(HaveOccurred())

					// Iterating the db while inserting new data entries at the mean time.
					errs := make([]error, 2)
					phi.ParBegin(func() {
						for iter.Next() {
							key, err := iter.Key()
							if err != nil {
								errs[0] = err
								return
							}
							value := testutil.TestStruct{D: []byte{}}
							err = iter.Value(&value)
							if err != nil {
								errs[0] = err
								return
							}

							stored, ok := allValues[key]
							if !ok {
								if err != nil {
									errs[0] = errors.New("test failed, iterator has new values inserted after the iterator been created ")
									return
								}
							}
							if !reflect.DeepEqual(value, stored) {
								if err != nil {
									errs[0] = errors.New("test failed, stored value has been changed")
									return
								}
							}
							delete(allValues, key)
						}
						Expect(len(allValues)).Should(BeZero())
					}, func() {
						// Inserting new data entries at the meantime.
						for i := 0; i < 20; i++ {
							newEntry := testutil.RandomTestStruct()
							err := table.Insert(fmt.Sprintf("key_%v", i), newEntry)
							if err != nil {
								errs[1] = err
								return
							}
						}
					})
					Expect(testutil.CheckErrors(errs))

					return true
				}

				Expect(quick.Check(iteration, nil)).NotTo(HaveOccurred())
			})
		})

		Context("negative tests", func() {
			It("should return error when trying to get key/value when the iterator doesn't have next value", func() {
				iteration := func(key string, value testutil.TestStruct) bool {
					table := NewTable(codec)
					if key == "" {
						return true
					}
					Expect(table.Insert("key", value)).Should(Succeed())

					iter, err := table.Iterator()
					Expect(err).NotTo(HaveOccurred())
					for iter.Next() {
					}

					Expect(iter.Next()).Should(BeFalse())
					_, err = iter.Key()
					Expect(err).Should(Equal(db.ErrIndexOutOfRange))
					var val testutil.TestStruct
					err = iter.Value(&val)
					Expect(err).Should(Equal(db.ErrIndexOutOfRange))

					return true
				}

				Expect(quick.Check(iteration, nil)).NotTo(HaveOccurred())
			})

			It("should return error when trying to get key/value without calling Next()", func() {
				iteration := func(key string) bool {
					table := NewTable(codec)
					if key == "" {
						return true
					}

					iter, err := table.Iterator()
					Expect(err).NotTo(HaveOccurred())

					// Iterator return db.ErrIndexOutOfRange error when trying to get the key and value.
					_, err = iter.Key()
					Expect(err).Should(Equal(db.ErrIndexOutOfRange))
					var val testutil.TestStruct
					err = iter.Value(&val)
					Expect(err).Should(Equal(db.ErrIndexOutOfRange))
					Expect(iter.Next()).Should(BeFalse())

					return true
				}

				Expect(quick.Check(iteration, nil)).NotTo(HaveOccurred())
			})

			It("should return ErrEmptyKey when trying to insert a value with empty key", func() {
				iteration := func(value []byte) bool {
					table := NewTable(codec)
					Expect(table.Insert("", value)).Should(Equal(db.ErrEmptyKey))
					return true
				}

				Expect(quick.Check(iteration, nil)).NotTo(HaveOccurred())
			})
		})

		Context("when trying to insert or get values with empty key", func() {
			It("should return ErrEmptyKey", func() {
				test := func(value testutil.TestStruct) bool {
					table := NewTable(codec)

					err := table.Insert("", value)
					Expect(err).Should(Equal(db.ErrEmptyKey))

					err = table.Get("", &value)
					Expect(err).Should(Equal(db.ErrEmptyKey))

					return true
				}

				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})
	}
})

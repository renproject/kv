package badgerdb_test

import (
	"errors"
	"fmt"
	"reflect"
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/kv/badgerdb"

	"github.com/renproject/kv/codec"
	"github.com/renproject/kv/db"
	"github.com/renproject/kv/testutil"
	"github.com/renproject/phi"
)

var codecs = []db.Codec{
	codec.JSONCodec,
	codec.GobCodec,
}

var _ = Describe("badger db implementation of the table", func() {

	Context("when creating a table", func() {
		It("should failed when passing a nil codec", func() {
			Expect(func() {
				NewTable("table", bdb, nil)
			}).Should(Panic())
		})
	})

	for i := range codecs {
		codec := codecs[i]

		Context(fmt.Sprintf("when reading and writing using %s codec", codec), func() {
			It("should be able read and write value", func() {
				readAndWrite := func(name, key string, value testutil.TestStruct) bool {
					table := NewTable(name, bdb, codec)
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

			It("should be able to iterate through the db using the iterator", func() {
				iteration := func(name string, values []testutil.TestStruct) bool {
					table := NewTable(name, bdb, codec)

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
						Expect(table.Delete(key)).Should(Succeed())
					}
					Expect(len(allValues)).Should(BeZero())

					return true
				}

				Expect(quick.Check(iteration, nil)).NotTo(HaveOccurred())
			})

			It("should return a iterator which only has the view of the table at the time been created", func() {
				iteration := func(name string, values []testutil.TestStruct) bool {
					table := NewTable(name, bdb, codec)

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
						errs[0] = func() error {
							for iter.Next() {
								key, err := iter.Key()
								if err != nil {
									return nil
								}
								value := testutil.TestStruct{D: []byte{}}
								err = iter.Value(&value)
								if err != nil {
									return nil
								}

								stored, ok := allValues[key]
								if !ok {
									if err != nil {
										return errors.New("test failed, iterator has new values inserted after the iterator been created ")
									}
								}
								if !reflect.DeepEqual(value, stored) {
									if err != nil {
										return errors.New("test failed, stored value has been changed")
									}
								}
								delete(allValues, key)
							}
							Expect(len(allValues)).Should(BeZero())
							return nil
						}()
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

		Context("when trying to read value from store", func() {
			It("should return error if the value object is not a pointer ", func() {
				read := func(name, key string, value testutil.TestStruct) bool {
					table := NewTable(name, bdb, codec)
					if key == "" {
						return true
					}
					Expect(table.Insert(key, value)).Should(Succeed())

					Expect(func() error {
						val := testutil.TestStruct{D: []byte{}}
						if err := table.Get(key, val); err != nil {
							return err
						}
						if !reflect.DeepEqual(val, value) {
							return errors.New("fail to get value from table")
						}
						return nil
					}()).ShouldNot(Succeed())

					return true
				}

				Expect(quick.Check(read, nil)).NotTo(HaveOccurred())
			})
		})

		Context("when trying to get key/value when the iterator doesn't have next value", func() {
			It("should return ErrIndexOutOfRange", func() {
				iteration := func(name, key string, value testutil.TestStruct) bool {
					table := NewTable(name, bdb, codec)
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
		})

		Context("when trying to get key/value without calling Next()", func() {
			It("should return ErrIndexOutOfRange ", func() {
				iteration := func(name, key string) bool {
					table := NewTable(name, bdb, codec)
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

		})

		Context("when trying to insert or get values with empty key", func() {
			It("should return ErrEmptyKey", func() {
				test := func(name string, value testutil.TestStruct) bool {
					table := NewTable(name, bdb, codec)

					Expect(table.Insert("", value)).Should(Equal(db.ErrEmptyKey))
					Expect(table.Get("", &value)).Should(Equal(db.ErrEmptyKey))
					Expect(table.Delete("")).Should(Equal(db.ErrEmptyKey))

					return true
				}

				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})
	}
})

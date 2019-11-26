package memdb_test

import (
	"errors"
	"fmt"
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

	for i := range testutil.Codecs {
		codec := testutil.Codecs[i]

		Context("when doing operation on a in-memory implementation of DB", func() {
			It("should be able to do read, write and delete", func() {
				memdb := New(codec)
				defer memdb.Close()

				test := func(name string, key string, value testutil.TestStruct) bool {
					// Will test empty in negative tests.
					if key == "" {
						return true
					}

					val := testutil.TestStruct{D: []byte{}}
					err := memdb.Get(key, &val)
					Expect(err).Should(Equal(db.ErrKeyNotFound))

					// Should be able to read the values after inserting.
					Expect(memdb.Insert(key, value)).NotTo(HaveOccurred())
					err = memdb.Get(key, &val)
					Expect(err).NotTo(HaveOccurred())
					Expect(reflect.DeepEqual(val, value)).Should(BeTrue())

					// Expect no values exists after deleting the values.
					Expect(memdb.Delete(key)).NotTo(HaveOccurred())
					err = memdb.Get(key, &val)
					Expect(err).Should(Equal(db.ErrKeyNotFound))

					return true
				}

				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})

			It("should be able to iterable through the db using the iterator", func() {
				memdb := New(codec)
				defer memdb.Close()

				iteration := func(name string, values []testutil.TestStruct) bool {
					// Insert all values and make a map for validation.
					allValues := map[string]testutil.TestStruct{}
					for i, value := range values {
						key := fmt.Sprintf("%v%v", name, i)
						Expect(memdb.Insert(key, value)).NotTo(HaveOccurred())
						allValues[fmt.Sprintf("%v", i)] = value
					}

					size, err := memdb.Size(name)
					Expect(err).NotTo(HaveOccurred())
					Expect(size).Should(Equal(len(values)))

					// Expect iterator gives us all the key-values pairs we insert.
					iter := memdb.Iterator(name)
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
						Expect(memdb.Delete(name + key))
					}
					return len(allValues) == 0
				}

				Expect(quick.Check(iteration, nil)).NotTo(HaveOccurred())
			})
		})

		Context("when doing multiple operations on the DB concurrently", func() {
			It("should work properly when doing reading and writing", func() {
				readAndWrite := func(values []testutil.TestStruct) bool {
					memdb := New(codec)
					defer memdb.Close()

					// Should be able to concurrently read and write data of different data.
					errs := make([]error, len(values))
					phi.ParForAll(len(values), func(i int) {
						errs[i] = func() error {
							value := values[i]
							key := fmt.Sprintf("key_%v", value.A)

							// Should be able to read the values after inserting.
							if err := memdb.Insert(key, value); err != nil {
								return err
							}

							val := testutil.TestStruct{D: []byte{}}
							if err := memdb.Get(key, &val); err != nil {
								return err
							}
							if !reflect.DeepEqual(val, value) {
								return errors.New("stored value are different")
							}

							// Expect no values exists after deleting the values.
							if err := memdb.Delete(key); err != nil {
								return err
							}
							if err := memdb.Get(key, &val); err != db.ErrKeyNotFound {
								return errors.New("fail to delete the data")
							}
							return nil
						}()
					})
					return testutil.CheckErrors(errs) == nil
				}

				Expect(quick.Check(readAndWrite, &quick.Config{MaxCount: 1})).NotTo(HaveOccurred())
			})
		})

		Context("when operating with empty key", func() {
			It("should return ErrEmptyKey error", func() {
				memdb := New(codec)
				defer memdb.Close()

				test := func(name string) bool {
					err := memdb.Insert("", "")
					Expect(err).Should(Equal(db.ErrEmptyKey))

					var val string
					err = memdb.Get("", &val)
					Expect(err).Should(Equal(db.ErrEmptyKey))

					err = memdb.Delete("")
					Expect(err).Should(Equal(db.ErrEmptyKey))

					return true
				}

				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})

		Context("when iterating through the db with a prefix", func() {
			Context("when trying get the key with an invalid index", func() {
				It("should return an ErrIndexOutOfRange error ", func() {
					memdb := New(codec)
					defer memdb.Close()

					iteration := func(name string, values []testutil.TestStruct) bool {
						for i, value := range values {
							Expect(memdb.Insert(fmt.Sprintf("%v%d", name, i), value)).Should(Succeed())
						}

						iter := memdb.Iterator(name)
						defer iter.Close()

						var val testutil.TestStruct
						_, err := iter.Key()
						Expect(err).Should(Equal(db.ErrIndexOutOfRange))
						Expect(iter.Value(&val)).Should(Equal(db.ErrIndexOutOfRange))

						for iter.Next() {
						}

						_, err = iter.Key()
						Expect(err).Should(Equal(db.ErrIndexOutOfRange))
						Expect(iter.Value(&val)).Should(Equal(db.ErrIndexOutOfRange))

						return true
					}

					Expect(quick.Check(iteration, nil)).NotTo(HaveOccurred())
				})
			})
		})
	}

	Context("when initializing the db with a nil codec", func() {
		It("should panic", func() {
			Expect(func() {
				New(nil)
			}).Should(Panic())
		})
	})
})

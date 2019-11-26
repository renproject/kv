package badgerdb_test

import (
	"fmt"
	"reflect"
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/kv/badgerdb"

	"github.com/renproject/kv/db"
	"github.com/renproject/kv/testutil"
)

var _ = Describe("badger DB implementation of the db", func() {

	for i := range testutil.Codecs {
		codec := testutil.Codecs[i]

		Context("when doing operation on a badgerDB implementation of DB", func() {
			It("should be able to do read, write and delete", func() {
				badgerDB := New(".badgerdb", codec)
				defer badgerDB.Close()

				readAndWrite := func(name string, key string, value testutil.TestStruct) bool {
					// Make sure the key is not nil
					if key == "" {
						return true
					}
					val := testutil.TestStruct{D: []byte{}}
					err := badgerDB.Get(key, &val)
					Expect(err).Should(Equal(db.ErrKeyNotFound))

					// Should be able to read the value after inserting.
					Expect(badgerDB.Insert(key, value)).NotTo(HaveOccurred())
					err = badgerDB.Get(key, &val)
					Expect(err).NotTo(HaveOccurred())
					Expect(reflect.DeepEqual(val, value)).Should(BeTrue())

					// Expect no value exists after deleting the value.
					Expect(badgerDB.Delete(key)).NotTo(HaveOccurred())
					err = badgerDB.Get(key, &val)
					Expect(err).Should(Equal(db.ErrKeyNotFound))

					return true
				}

				Expect(quick.Check(readAndWrite, nil)).NotTo(HaveOccurred())
			})

			It("should be able to iterable through the db using the iterator", func() {
				badgerDB := New(".badgerdb", codec)
				defer badgerDB.Close()

				iteration := func(name string, values []testutil.TestStruct) bool {
					// Insert all values and make a map for validation.
					allValues := map[string]testutil.TestStruct{}
					for i, value := range values {
						key := fmt.Sprintf("%v%v", name, i)
						Expect(badgerDB.Insert(key, value)).NotTo(HaveOccurred())
						allValues[fmt.Sprintf("%v", i)] = value
					}

					size, err := badgerDB.Size(name)
					Expect(err).NotTo(HaveOccurred())
					Expect(size).Should(Equal(len(values)))

					// Expect iterator gives us all the key-value pairs we insert.
					iter := badgerDB.Iterator(name)
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
						Expect(badgerDB.Delete(name + key)).Should(Succeed())
					}
					return len(allValues) == 0
				}

				Expect(quick.Check(iteration, nil)).NotTo(HaveOccurred())
			})
		})

		Context("when operating with empty key", func() {
			It("should return ErrEmptyKey error", func() {
				badgerDB := New(".badgerdb", codec)
				defer badgerDB.Close()

				test := func() bool {
					err := badgerDB.Insert("", []byte{})
					Expect(err).Should(Equal(db.ErrEmptyKey))

					var val string
					err = badgerDB.Get("", &val)
					Expect(err).Should(Equal(db.ErrEmptyKey))

					err = badgerDB.Delete("")
					Expect(err).Should(Equal(db.ErrEmptyKey))

					return true
				}

				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})

		Context("when iterating through the db with a prefix", func() {
			Context("when trying get the key with an invalid index", func() {
				It("should return an ErrIndexOutOfRange error ", func() {
					badgerDB := New(".badgerdb", codec)
					defer badgerDB.Close()

					iteration := func(name string, values []testutil.TestStruct) bool {
						for i, value := range values {
							Expect(badgerDB.Insert(fmt.Sprintf("%v%d", name, i), value)).Should(Succeed())
						}

						iter := badgerDB.Iterator(name)
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

						for i := range values {
							Expect(badgerDB.Delete(fmt.Sprintf("%d", i))).Should(Succeed())
						}

						return true
					}

					Expect(quick.Check(iteration, nil)).NotTo(HaveOccurred())
				})
			})
		})

		Context("when trying to create more than one db using the same path", func() {
			It("should panic", func() {
				badgerDB := New(".badgerdb", codec)
				defer badgerDB.Close()

				Expect(func() {
					New(".badgerdb", codec)
				}).Should(Panic())
			})
		})
	}

	Context("when initializing the db with a nil codec", func() {
		It("should panic", func() {
			Expect(func() {
				New("dir", nil)
			}).Should(Panic())
		})
	})
})

package leveldb_test

import (
	"fmt"
	"reflect"
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/kv/leveldb"

	"github.com/renproject/kv/db"
	"github.com/renproject/kv/testutil"
)

var _ = Describe("level DB implementation of the db", func() {

	for i := range testutil.Codecs {
		codec := testutil.Codecs[i]

		Context("when doing operation on a leveldb implementation of DB ", func() {
			It("should be able to do read, write and delete", func() {
				levelDB := New(".leveldb", codec)
				defer levelDB.Close()

				readAndWrite := func(name string, key string, value testutil.TestStruct) bool {
					// Make sure the key is not nil
					if key == "" {
						return true
					}
					val := testutil.TestStruct{D: []byte{}}
					err := levelDB.Get(key, &val)
					Expect(err).Should(Equal(db.ErrKeyNotFound))

					// Should be able to read the value after inserting.
					Expect(levelDB.Insert(key, value)).NotTo(HaveOccurred())
					err = levelDB.Get(key, &val)
					Expect(err).NotTo(HaveOccurred())
					Expect(reflect.DeepEqual(val, value)).Should(BeTrue())

					// Expect no value exists after deleting the value.
					Expect(levelDB.Delete(key)).NotTo(HaveOccurred())
					err = levelDB.Get(key, &val)
					Expect(err).Should(Equal(db.ErrKeyNotFound))

					return true
				}

				Expect(quick.Check(readAndWrite, nil)).NotTo(HaveOccurred())
			})

			It("should be able to iterable through the db using the iter", func() {
				levelDB := New(".leveldb", codec)
				defer levelDB.Close()

				iteration := func(name string, values []testutil.TestStruct) bool {
					// Insert all values and make a map for validation.
					allValues := map[string]testutil.TestStruct{}
					for i, value := range values {
						key := fmt.Sprintf("%v%v", name, i)
						Expect(levelDB.Insert(key, value)).NotTo(HaveOccurred())
						allValues[fmt.Sprintf("%v", i)] = value
					}

					size, err := levelDB.Size(name)
					Expect(err).NotTo(HaveOccurred())
					Expect(size).Should(Equal(len(values)))

					// Expect iter gives us all the key-value pairs we insert.
					iter := levelDB.Iterator(name)
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
						Expect(levelDB.Delete(name + key)).Should(Succeed())
					}
					return len(allValues) == 0
				}

				Expect(quick.Check(iteration, nil)).NotTo(HaveOccurred())
			})
		})

		Context("when operating with empty key", func() {
			It("should return ErrEmptyKey error", func() {
				levelDB := New(".leveldb", codec)
				defer levelDB.Close()

				test := func() bool {
					err := levelDB.Insert("", "")
					Expect(err).Should(Equal(db.ErrEmptyKey))

					var val string
					err = levelDB.Get("", &val)
					Expect(err).Should(Equal(db.ErrEmptyKey))

					err = levelDB.Delete("")
					Expect(err).Should(Equal(db.ErrEmptyKey))

					return true
				}

				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})

		Context("when iterating through the db with a prefix", func() {
			Context("when trying get the key with an invalid index", func() {
				It("should return an ErrIndexOutOfRange error ", func() {
					levelDB := New(".leveldb", codec)
					defer levelDB.Close()

					iteration := func(name string, values []testutil.TestStruct) bool {
						// Inserting some data into the db
						for i, value := range values {
							Expect(levelDB.Insert(fmt.Sprintf("%v%d", name, i), value)).Should(Succeed())
						}

						// Try to get key and value without calling next.
						iter := levelDB.Iterator(name)
						Expect(iter).ShouldNot(BeNil())
						defer iter.Close()

						var val testutil.TestStruct
						_, err := iter.Key()
						Expect(err).Should(Equal(db.ErrIndexOutOfRange))
						Expect(iter.Value(&val)).Should(Equal(db.ErrIndexOutOfRange))

						for iter.Next() {
						}

						// Try to get key and value when next returns false.
						_, err = iter.Key()
						Expect(err).Should(Equal(db.ErrIndexOutOfRange))
						Expect(iter.Value(&val)).Should(Equal(db.ErrIndexOutOfRange))

						for i := range values {
							Expect(levelDB.Delete(fmt.Sprintf("%d", i))).Should(Succeed())
						}

						return true
					}

					Expect(quick.Check(iteration, nil)).NotTo(HaveOccurred())
				})
			})
		})

		Context("when trying to create more than one db using the same path", func() {
			It("should panic", func() {
				levelDB := New(".leveldb", codec)
				defer levelDB.Close()

				Expect(func() {
					New(".leveldb", codec)
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

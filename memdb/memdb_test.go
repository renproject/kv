package memdb_test

import (
	"bytes"
	"math/rand"
	"reflect"
	"testing/quick"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/kv"
)

const TimeToLive = 30

type testStruct struct {
	A string
	B int
	C bool
	D []byte
	E map[string]float64
}

func randomTestStruct(ran *rand.Rand) testStruct {
	t := reflect.TypeOf(testStruct{})
	vaule, _ := quick.Value(t, ran)
	return vaule.Interface().(testStruct)
}

var _ = Describe("Cache implementation of Store", func() {
	Context("when reading and writing", func() {
		It("should be able read and write value without any error", func() {
			readAndWrite := func(key string, value testStruct) bool {
				cache := NewCache()

				var newValue testStruct
				Expect(cache.Read(key, &newValue)).Should(Equal(ErrKeyNotFound))
				Expect(cache.Write(key, value)).NotTo(HaveOccurred())

				Expect(cache.Read(key, &newValue)).NotTo(HaveOccurred())
				Expect(reflect.DeepEqual(value, newValue)).Should(BeTrue())

				Expect(cache.Delete(key)).NotTo(HaveOccurred())
				Expect(cache.Read(key, &newValue)).Should(Equal(ErrKeyNotFound))

				return true
			}

			Expect(quick.Check(readAndWrite, nil)).NotTo(HaveOccurred())
		})

		It("should be able to read and write data in bytes directly", func() {
			readWrite := func(key string, value []byte) bool {
				cache := NewCache()
				_, err := cache.ReadData(key)
				Expect(err).Should(Equal(ErrKeyNotFound))

				Expect(cache.WriteData(key, value)).NotTo(HaveOccurred())
				data, err := cache.ReadData(key)
				Expect(err).NotTo(HaveOccurred())
				return bytes.Compare(data, value) == 0
			}

			Expect(quick.Check(readWrite, nil)).NotTo(HaveOccurred())
		})
	})
})

var _ = Describe("Cache implementation of IterableStore", func() {
	Context("when reading and writing with data-expiration", func() {
		It("should be able to store a struct with pre-defined value type", func() {
			readAndWrite := func(key string, value testStruct) bool {
				cache := NewIterableCache(TimeToLive)
				entries, err := cache.Entries()
				Expect(err).NotTo(HaveOccurred())
				Expect(entries).Should(Equal(0))

				var newValue testStruct
				Expect(cache.Read(key, &newValue)).Should(Equal(ErrKeyNotFound))
				Expect(cache.Write(key, value)).NotTo(HaveOccurred())

				Expect(cache.Read(key, &newValue)).NotTo(HaveOccurred())
				Expect(reflect.DeepEqual(value, newValue)).Should(BeTrue())
				entries, err = cache.Entries()
				Expect(err).NotTo(HaveOccurred())
				Expect(entries).Should(Equal(1))

				Expect(cache.Delete(key)).NotTo(HaveOccurred())
				Expect(cache.Read(key, &newValue)).Should(Equal(ErrKeyNotFound))
				entries, err = cache.Entries()
				Expect(err).NotTo(HaveOccurred())
				Expect(entries).Should(Equal(0))
				return true
			}

			Expect(quick.Check(readAndWrite, nil)).NotTo(HaveOccurred())
		})

		It("should be able to return the number of entries in the store ", func() {
			ran := rand.New(rand.NewSource(time.Now().Unix()))

			addingData := func() bool {
				cache := NewIterableCache(TimeToLive)
				num := rand.Intn(128)
				for i := 0; i < num; i++ {
					value := randomTestStruct(ran)
					value.A = string(i)
					Expect(cache.Write(value.A, value)).NotTo(HaveOccurred())
				}
				entries, err := cache.Entries()
				Expect(err).NotTo(HaveOccurred())
				return entries == num
			}

			Expect(quick.Check(addingData, nil)).NotTo(HaveOccurred())
		})

		It("should be able to read and write data in bytes directly", func() {
			readWrite := func(key string, value []byte) bool {
				cache := NewIterableCache(TimeToLive)
				_, err := cache.ReadData(key)
				Expect(err).Should(Equal(ErrKeyNotFound))

				Expect(cache.WriteData(key, value)).NotTo(HaveOccurred())
				data, err := cache.ReadData(key)
				Expect(err).NotTo(HaveOccurred())
				return bytes.Compare(data, value) == 0
			}

			Expect(quick.Check(readWrite, nil)).NotTo(HaveOccurred())
		})
	})

	Context("when reading and writing without data-expiration", func() {
		It("should be able to store a struct with pre-defined value type", func() {
			readAndWrite := func(key string, value testStruct) bool {
				cache := NewIterableCache(0)
				Expect(cache.Entries()).Should(Equal(0))

				var newValue testStruct
				Expect(cache.Read(key, &newValue)).Should(Equal(ErrKeyNotFound))
				Expect(cache.Write(key, value)).NotTo(HaveOccurred())

				Expect(cache.Read(key, &newValue)).NotTo(HaveOccurred())
				Expect(reflect.DeepEqual(value, newValue)).Should(BeTrue())
				Expect(cache.Entries()).Should(Equal(1))

				Expect(cache.Delete(key)).NotTo(HaveOccurred())
				Expect(cache.Read(key, &newValue)).Should(Equal(ErrKeyNotFound))
				Expect(cache.Entries()).Should(Equal(0))
				return true
			}

			Expect(quick.Check(readAndWrite, nil)).NotTo(HaveOccurred())
		})

		It("should be able to return the number of entries in the store ", func() {
			ran := rand.New(rand.NewSource(time.Now().Unix()))

			addingData := func() bool {
				cache := NewIterableCache(0)
				num := rand.Intn(128)
				for i := 0; i < num; i++ {
					value := randomTestStruct(ran)
					value.A = string(i)
					Expect(cache.Write(value.A, value)).NotTo(HaveOccurred())
				}

				entries, err := cache.Entries()
				Expect(err).NotTo(HaveOccurred())
				return entries == num
			}

			Expect(quick.Check(addingData, nil)).NotTo(HaveOccurred())
		})
	})

	Context("when iterating the data in the store", func() {
		It("should iterate through all the key-values in the store", func() {
			ran := rand.New(rand.NewSource(time.Now().Unix()))

			iterating := func() bool {
				cache := NewIterableCache(TimeToLive)

				entries, err := cache.Entries()
				Expect(err).NotTo(HaveOccurred())
				Expect(entries).Should(Equal(0))
				num := rand.Intn(128)
				allData := map[string]testStruct{}
				for i := 0; i < num; i++ {
					value := randomTestStruct(ran)
					allData[value.A] = value
					Expect(cache.Write(value.A, value)).NotTo(HaveOccurred())
				}
				entries, err = cache.Entries()
				Expect(err).NotTo(HaveOccurred())
				Expect(entries).Should(Equal(len(allData)))

				iter := cache.Iterator()
				for iter.Next() {
					var wrongType []byte
					err := iter.Value(&wrongType)
					Expect(err).To(HaveOccurred())

					var value testStruct
					key, err := iter.Key()
					Expect(err).NotTo(HaveOccurred())
					err = iter.Value(&value)
					_, ok := allData[key]
					Expect(ok).Should(BeTrue())
					Expect(cache.Delete(key)).NotTo(HaveOccurred())
					delete(allData, key)
				}

				entries, err = cache.Entries()
				Expect(err).NotTo(HaveOccurred())
				Expect(entries).Should(Equal(0))
				return len(allData) == 0
			}
			Expect(quick.Check(iterating, nil)).NotTo(HaveOccurred())
		})

		It("should return error when there is no next key-value pair", func() {
			iterating := func(key string, value testStruct) bool {
				cache := NewIterableCache(TimeToLive)
				Expect(cache.Write(key, value)).NotTo(HaveOccurred())
				iter := cache.Iterator()
				for iter.Next() {
				}

				key, err := iter.Key()
				Expect(err).To(Equal(ErrNoMoreItems))
				var val testStruct
				err = iter.Value(&val)
				return err == ErrNoMoreItems
			}
			Expect(quick.Check(iterating, nil)).NotTo(HaveOccurred())
		})
	})

	Context("when querying data which is expired", func() {
		It("should return ErrDataExpired", func() {
			ran := rand.New(rand.NewSource(time.Now().Unix()))
			value := randomTestStruct(ran)
			cache := NewIterableCache(1)
			Expect(cache.Write(value.A, value)).NotTo(HaveOccurred())

			time.Sleep(2 * time.Second)
			var newValue testStruct
			Expect(cache.Read(value.A, &newValue)).Should(Equal(ErrDataExpired))
		})
	})

	Context("when giving wrong data type of the value", func() {
		It("should return an error", func() {
			wrongType := func(key string, value testStruct) bool {
				cache := NewIterableCache(TimeToLive)
				Expect(cache.Write(value.A, value)).NotTo(HaveOccurred())

				var wrongType []byte
				return cache.Read(value.A, &wrongType) != nil
			}
			Expect(quick.Check(wrongType, nil)).NotTo(HaveOccurred())
		})
	})

	Context("when trying to store some data which is no marshalable", func() {
		It("should fail and return an error", func() {
			key, value := "key", make(chan struct{})
			cache := NewIterableCache(TimeToLive)
			Expect(cache.Write(key, value)).To(HaveOccurred())
		})
	})
})

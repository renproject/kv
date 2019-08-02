package cache_test

//
// import (
// 	"fmt"
// 	"math/rand"
// 	"reflect"
// 	"testing/quick"
// 	"time"
//
// 	. "github.com/onsi/ginkgo"
// 	. "github.com/onsi/gomega"
// 	. "github.com/renproject/kv/cache"
//
// 	"github.com/renproject/kv/db"
// 	"github.com/renproject/kv/json"
// 	"github.com/renproject/kv/memdb"
// )
//
// var Ran = rand.New(rand.NewSource(time.Now().Unix()))
//
// type testStruct struct {
// 	A string
// 	B int
// 	C bool
// 	D []byte
// 	E map[string]float64
// }
//
// func randomTestStruct(ran *rand.Rand) testStruct {
// 	t := reflect.TypeOf(testStruct{})
// 	vaule, ok := quick.Value(t, ran)
// 	Expect(ok).Should(BeTrue())
// 	return vaule.Interface().(testStruct)
// }
//
// var _ = Describe("ttl store", func() {
// 	Context("when reading and writing", func() {
// 		It("should be able read and write value without any error", func() {
// 			readAndWrite := func(key string, value testStruct) bool {
// 				if key == "" {
// 					return true
// 				}
// 				cache, err := NewTTL(json.New(memdb.New()), time.Second)
// 				Expect(err).NotTo(HaveOccurred())
//
// 				var newValue testStruct
// 				Expect(cache.Get(key, &newValue)).Should(Equal(db.ErrKeyNotFound))
// 				Expect(cache.Insert(key, value)).NotTo(HaveOccurred())
//
// 				Expect(cache.Get(key, &newValue)).NotTo(HaveOccurred())
// 				Expect(reflect.DeepEqual(value, newValue)).Should(BeTrue())
//
// 				Expect(cache.Delete(key)).NotTo(HaveOccurred())
// 				Expect(cache.Get(key, &newValue)).Should(Equal(db.ErrKeyNotFound))
//
// 				return true
// 			}
//
// 			Expect(quick.Check(readAndWrite, nil)).NotTo(HaveOccurred())
// 		})
// 	})
//
// 	Context("when iterating", func() {
// 		It("should be able to return the correct number of values in the store", func() {
// 			iterating := func(key string, value testStruct) bool {
// 				cache, err := NewTTL(json.New(memdb.New()), 100*time.Millisecond)
// 				Expect(err).NotTo(HaveOccurred())
//
// 				// Expect the initial size to be 0.
// 				size, err := cache.Size()
// 				Expect(err).NotTo(HaveOccurred())
// 				Expect(size).Should(Equal(0))
//
// 				// Insert random number of values into the store.
// 				num := rand.Intn(128)
// 				allData := map[string]testStruct{}
// 				for i := 0; i < num; i++ {
// 					value := randomTestStruct(Ran)
// 					value.A = fmt.Sprintf("%v", i)
// 					allData[value.A] = value
// 					Expect(cache.Insert(value.A, value)).NotTo(HaveOccurred())
// 				}
//
// 				// Expect the size to be the number of value we inserted.
// 				size, err = cache.Size()
// 				Expect(err).NotTo(HaveOccurred())
// 				Expect(size).Should(Equal(num))
//
// 				// Expect the size to be 0 as all values should expired.
// 				time.Sleep(100 * time.Millisecond)
// 				size, err = cache.Size()
// 				Expect(err).NotTo(HaveOccurred())
// 				return size == 0
// 			}
//
// 			Expect(quick.Check(iterating, nil)).NotTo(HaveOccurred())
// 		})
//
// 		It("should be able iterate through the store", func() {
// 			iterating := func(key string, value testStruct) bool {
// 				cache, err := NewTTL(json.New(memdb.New()), time.Second)
// 				Expect(err).NotTo(HaveOccurred())
//
// 				// Insert random number of values into the store.
// 				num := rand.Intn(128)
// 				allData := map[string]testStruct{}
// 				for i := 0; i < num; i++ {
// 					value := randomTestStruct(Ran)
// 					value.A = fmt.Sprintf("%v", i)
// 					allData[value.A] = value
// 					Expect(cache.Insert(value.A, value)).NotTo(HaveOccurred())
// 				}
//
// 				// Expect the iterator to be able to give us all values.
// 				iter, err := cache.Iterator()
// 				Expect(err).NotTo(HaveOccurred())
// 				for iter.Next() {
// 					var wrongType []byte
// 					err := iter.Value(&wrongType)
// 					Expect(err).To(HaveOccurred())
//
// 					var value testStruct
// 					key, err := iter.Key()
// 					Expect(err).NotTo(HaveOccurred())
// 					err = iter.Value(&value)
// 					_, ok := allData[key]
// 					Expect(ok).Should(BeTrue())
// 					Expect(cache.Delete(key)).NotTo(HaveOccurred())
// 					delete(allData, key)
// 				}
//
// 				// Expect the size to be the number of value we inserted.
// 				size, err := cache.Size()
// 				Expect(err).NotTo(HaveOccurred())
// 				Expect(size).Should(Equal(0))
// 				return len(allData) == 0
// 			}
//
// 			Expect(quick.Check(iterating, nil)).NotTo(HaveOccurred())
// 		})
//
// 		It("should only give us valid data when iterating", func() {
// 			iterating := func(key string, value testStruct) bool {
// 				cache, err := NewTTL(json.New(memdb.New()), 100*time.Millisecond)
// 				Expect(err).NotTo(HaveOccurred())
//
// 				// Insert random number of values into the store.
// 				num := rand.Intn(128)
// 				allData := map[string]testStruct{}
// 				for i := 0; i < num; i++ {
// 					value := randomTestStruct(Ran)
// 					value.A = fmt.Sprintf("%v", i)
// 					allData[value.A] = value
// 					Expect(cache.Insert(value.A, value)).NotTo(HaveOccurred())
// 				}
//
// 				time.Sleep(100 * time.Millisecond)
// 				iter, err := cache.Iterator()
// 				Expect(err).NotTo(HaveOccurred())
// 				Expect(iter.Next()).Should(BeFalse())
// 				return true
// 			}
//
// 			Expect(quick.Check(iterating, nil)).NotTo(HaveOccurred())
// 		})
// 	})
//
// 	Context("when reading and writing with data-expiration", func() {
// 		It("should be able to store a struct with pre-defined value type", func() {
// 			readAndWrite := func(key string, value testStruct) bool {
// 				if key == "" {
// 					return true
// 				}
//
// 				cache, err := NewTTL(json.New(memdb.New()), 100*time.Millisecond)
// 				Expect(err).NotTo(HaveOccurred())
//
// 				var newValue testStruct
// 				Expect(cache.Get(key, &newValue)).Should(Equal(db.ErrKeyNotFound))
// 				Expect(cache.Insert(key, value)).NotTo(HaveOccurred())
//
// 				Expect(cache.Get(key, &newValue)).NotTo(HaveOccurred())
// 				Expect(reflect.DeepEqual(value, newValue)).Should(BeTrue())
//
// 				time.Sleep(100 * time.Millisecond)
// 				Expect(cache.Get(key, &newValue)).To(Equal(ErrExpired))
//
// 				return true
// 			}
//
// 			Expect(quick.Check(readAndWrite, nil)).NotTo(HaveOccurred())
// 		})
//
// 		It("should read all data stored in the store when initializing", func() {
// 			readAndWrite := func(key string, value testStruct) bool {
// 				if key == "" {
// 					return true
// 				}
// 				store := json.New(memdb.New())
// 				Expect(store.Insert(key, value)).NotTo(HaveOccurred())
//
// 				cache, err := NewTTL(store, 10*time.Second)
// 				Expect(err).NotTo(HaveOccurred())
//
// 				var newValue testStruct
// 				Expect(cache.Get(key, &newValue)).NotTo(HaveOccurred())
// 				Expect(reflect.DeepEqual(value, newValue)).Should(BeTrue())
//
// 				return true
// 			}
//
// 			Expect(quick.Check(readAndWrite, nil)).NotTo(HaveOccurred())
// 		})
// 	})
// })

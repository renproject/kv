package badgerdb_test

//
// import (
// 	"bytes"
// 	"fmt"
// 	"os/exec"
// 	"testing/quick"
//
// 	"github.com/dgraph-io/badger"
// 	"github.com/renproject/kv/db"
//
// 	. "github.com/onsi/ginkgo"
// 	. "github.com/onsi/gomega"
// 	. "github.com/renproject/kv/badgerdb"
// )
//
// var _ = Describe("BadgerDB implementation of key-value Store", func() {
//
// 	initDB := func() *badger.DB {
// 		Expect(exec.Command("mkdir", "-p", ".badgerdb").Run()).NotTo(HaveOccurred())
// 		opts := badger.DefaultOptions("./.badgerdb")
// 		opts.Dir = "./.badgerdb"
// 		opts.ValueDir = "./.badgerdb"
// 		db, err := badger.Open(opts)
// 		Expect(err).NotTo(HaveOccurred())
// 		return db
// 	}
//
// 	closeDB := func(db *badger.DB) {
// 		Expect(db.Close()).NotTo(HaveOccurred())
// 		Expect(exec.Command("rm", "-rf", "./.badgerdb").Run()).NotTo(HaveOccurred())
// 	}
//
// 	Context("when reading and writing", func() {
// 		It("should be able read and write value", func() {
// 			badgerdb := initDB()
// 			defer closeDB(badgerdb)
//
// 			readAndWrite := func(key string, value []byte) bool {
// 				bdb := New(badgerdb)
// 				if key == "" {
// 					return true
// 				}
//
// 				// Expect no value exists in the db with the given key.
// 				_, err := bdb.Get(key)
// 				Expect(err).Should(Equal(db.ErrKeyNotFound))
//
// 				// Should be able to read the value after inserting.
// 				Expect(bdb.Insert(key, value)).NotTo(HaveOccurred())
// 				data, err := bdb.Get(key)
// 				Expect(err).NotTo(HaveOccurred())
// 				Expect(bytes.Compare(data, value)).Should(BeZero())
//
// 				// Expect no value exists after deleting the value.
// 				Expect(bdb.Delete(key)).NotTo(HaveOccurred())
// 				_, err = bdb.Get(key)
// 				return err == db.ErrKeyNotFound
// 			}
// 			Expect(quick.Check(readAndWrite, nil)).NotTo(HaveOccurred())
// 		})
//
// 		It("should be able to iterable through the db using the iterator", func() {
// 			badgerdb := initDB()
// 			defer closeDB(badgerdb)
//
// 			iteration := func(values [][]byte) bool {
// 				bdb := New(badgerdb)
//
// 				// Insert all values and make a map for validation.
// 				allValues := map[string][]byte{}
// 				for i, value := range values {
// 					key := fmt.Sprintf("%v", i)
// 					Expect(bdb.Insert(key, value)).NotTo(HaveOccurred())
// 					allValues[key] = value
// 				}
//
// 				// Expect db size to the number of values we insert.
// 				size, err := bdb.Size()
// 				Expect(err).NotTo(HaveOccurred())
// 				Expect(size).Should(Equal(len(values)))
//
// 				// Expect iterator gives us all the key-value pairs we insert.
// 				iter := bdb.Iterator()
// 				for iter.Next() {
// 					key, err := iter.Key()
// 					Expect(err).NotTo(HaveOccurred())
// 					value, err := iter.Value()
// 					Expect(err).NotTo(HaveOccurred())
// 					Expect(bdb.Delete(key)).NotTo(HaveOccurred())
//
// 					stored, ok := allValues[key]
// 					Expect(ok).Should(BeTrue())
// 					Expect(bytes.Compare(value, stored)).Should(BeZero())
// 					delete(allValues, key)
// 				}
// 				return len(allValues) == 0
// 			}
//
// 			Expect(quick.Check(iteration, nil)).NotTo(HaveOccurred())
// 		})
//
// 		It("should return error when trying to get key/value when the iterator doesn't have next value", func() {
// 			badgerdb := initDB()
// 			defer closeDB(badgerdb)
//
// 			iteration := func(key string, value []byte) bool {
// 				bdb := New(badgerdb)
// 				iter := bdb.Iterator()
//
// 				for iter.Next() {
// 				}
//
// 				_, err := iter.Key()
// 				Expect(err).Should(Equal(db.ErrIndexOutOfRange))
// 				_, err = iter.Value()
// 				Expect(err).Should(Equal(db.ErrIndexOutOfRange))
// 				return iter.Next() == false
// 			}
//
// 			Expect(quick.Check(iteration, nil)).NotTo(HaveOccurred())
// 		})
//
// 		It("should return error when trying to get key/value without calling Next()", func() {
// 			badgerdb := initDB()
// 			defer closeDB(badgerdb)
//
// 			iteration := func(key string, value []byte) bool {
// 				bdb := New(badgerdb)
// 				iter := bdb.Iterator()
//
// 				_, err := iter.Key()
// 				Expect(err).Should(Equal(db.ErrIndexOutOfRange))
// 				_, err = iter.Value()
// 				Expect(err).Should(Equal(db.ErrIndexOutOfRange))
// 				return iter.Next() == false
// 			}
//
// 			Expect(quick.Check(iteration, nil)).NotTo(HaveOccurred())
// 		})
//
// 		It("should return ErrEmptyKey when trying to insert a value with empty key", func() {
// 			badgerdb := initDB()
// 			defer closeDB(badgerdb)
//
// 			iteration := func(value []byte) bool {
// 				bdb := New(badgerdb)
// 				return bdb.Insert("", value) == db.ErrEmptyKey
// 			}
//
// 			Expect(quick.Check(iteration, nil)).NotTo(HaveOccurred())
// 		})
// 	})
// })

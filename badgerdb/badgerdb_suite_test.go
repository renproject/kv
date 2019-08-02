package badgerdb_test

import (
	"os/exec"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/dgraph-io/badger"
)

var bdb *badger.DB

func TestBadgerdb(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Badgerdb Suite")
}

// Creating a badgerDB instance before running the entire test suite.
var _ = BeforeSuite(func() {
	err := exec.Command("mkdir", "-p", ".badgerdb").Run()
	Expect(err).NotTo(HaveOccurred())
	opts := badger.DefaultOptions("./.badgerdb")
	bdb, err = badger.Open(opts)
	Expect(err).NotTo(HaveOccurred())
	time.Sleep(time.Second)
})

// Close and remove all the related files after finishing the test suite.
var _ = AfterSuite(func() {
	Expect(bdb.Close()).NotTo(HaveOccurred())
	Expect(exec.Command("rm", "-rf", "./.badgerdb").Run()).NotTo(HaveOccurred())
})

// Clean the badgerDB instance after each test
var _ = JustAfterEach(func() {
	Expect(bdb.DropAll()).Should(Succeed())
})

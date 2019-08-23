package leveldb_test

import (
	"os/exec"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestLeveldb(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Leveldb Suite")
}

// Creating a leveldb instance before running the entire test suite.
var _ = BeforeSuite(func() {
	err := exec.Command("mkdir", "-p", ".leveldb").Run()
	Expect(err).NotTo(HaveOccurred())
})

// Clean the levelDB instance after each test
var _ = JustAfterEach(func() {
	Expect(exec.Command("rm", "-rf", "./.leveldb").Run()).NotTo(HaveOccurred())
})

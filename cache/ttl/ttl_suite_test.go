package ttl_test

import (
	"os/exec"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestTtl(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Ttl Suite")
}

// Clean the badgerDB instance after each test
var _ = JustAfterEach(func() {
	Expect(exec.Command("rm", "-rf", "./.badgerdb").Run()).NotTo(HaveOccurred())
	Expect(exec.Command("rm", "-rf", "./.leveldb").Run()).NotTo(HaveOccurred())
})

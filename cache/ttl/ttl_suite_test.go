package ttl_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestTtl(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Ttl Suite")
}

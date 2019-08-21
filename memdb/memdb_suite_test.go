package memdb_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/renproject/kv/codec"
	"github.com/renproject/kv/db"
)

func TestMemdb(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Memdb Suite")
}

// Codecs we want to test.
var codecs = []db.Codec{
	codec.JSONCodec,
	codec.GobCodec,
}

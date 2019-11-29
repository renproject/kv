package codec_test

import (
	"bytes"
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/kv/codec"
)

var _ = Describe("Codec", func() {
	Context("Binary codec", func() {
		It("should be able to correctly encode/decode []byte", func() {
			test := func(obj []byte) bool {
				codec := BinaryCodec
				data, err := codec.Encode(obj)
				Expect(err).NotTo(HaveOccurred())

				var decoded []byte
				Expect(codec.Decode(data, &decoded)).NotTo(HaveOccurred())
				Expect(bytes.Equal(obj, decoded)).Should(BeTrue())
				return true
			}

			Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
		})
	})
})

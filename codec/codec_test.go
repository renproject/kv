package codec_test

import (
	"bytes"
	"reflect"
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/kv/codec"
	. "github.com/renproject/kv/testutil"
)

var _ = Describe("codec", func() {
	Context("binary codec", func() {
		It("should return the correct string", func() {
			codec := BinaryCodec
			Expect(codec.String()).To(Equal("binary"))
		})

		It("should be able to correctly encode/decode a custom struct", func() {
			test := func(obj TestStruct) bool {
				codec := BinaryCodec
				data, err := codec.Encode(obj)
				Expect(err).NotTo(HaveOccurred())

				var decoded TestStruct
				Expect(codec.Decode(data, &decoded)).NotTo(HaveOccurred())
				Expect(reflect.DeepEqual(obj, decoded)).Should(BeTrue())
				return true
			}

			Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
		})

		It("should be able to correctly encode/decode bytes", func() {
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

		It("should be able to correctly encode/decode int", func() {
			test := func(obj int64) bool {
				codec := BinaryCodec
				data, err := codec.Encode(obj)
				Expect(err).NotTo(HaveOccurred())

				var decoded int64
				Expect(codec.Decode(data, &decoded)).NotTo(HaveOccurred())
				Expect(decoded).To(Equal(obj))
				return true
			}

			Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
		})
	})

	Context("JSON codec", func() {
		It("should return the correct string", func() {
			codec := JSONCodec
			Expect(codec.String()).To(Equal("json"))
		})

		It("should be able to correctly encode/decode a custom struct", func() {
			test := func(obj TestStruct) bool {
				codec := JSONCodec
				data, err := codec.Encode(obj)
				Expect(err).NotTo(HaveOccurred())

				var decoded TestStruct
				Expect(codec.Decode(data, &decoded)).NotTo(HaveOccurred())
				Expect(reflect.DeepEqual(obj, decoded)).Should(BeTrue())
				return true
			}

			Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
		})
	})

	Context("Gob codec", func() {
		It("should return the correct string", func() {
			codec := GobCodec
			Expect(codec.String()).To(Equal("gob"))
		})

		It("should be able to correctly encode/decode a custom struct", func() {
			test := func(obj TestStruct) bool {
				codec := GobCodec
				data, err := codec.Encode(obj)
				Expect(err).NotTo(HaveOccurred())

				var decoded TestStruct
				Expect(codec.Decode(data, &decoded)).NotTo(HaveOccurred())
				Expect(reflect.DeepEqual(obj, decoded)).Should(BeTrue())
				return true
			}

			Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
		})
	})
})

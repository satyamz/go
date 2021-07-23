package xdr

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewDecoratedSiganture(t *testing.T) {
	ds := NewDecoratedSignature(
		[]byte{0, 1, 2, 3, 4, 5, 6, 7, 8},
		[4]byte{9, 10, 11, 12},
	)
	assert.Equal(
		t,
		DecoratedSignature{
			Hint:      [4]byte{9, 10, 11, 12},
			Signature: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8},
		},
		ds,
	)
}

func TestNewDecoratedSigantureForPayload_payload4OrMore(t *testing.T) {
	ds := NewDecoratedSignatureForPayload(
		[]byte{0, 1, 2, 3, 4, 5, 6, 7, 8},
		[4]byte{9, 10, 11, 12},
		[]byte{13, 14, 15, 16, 17, 18, 19, 20, 21},
	)
	assert.Equal(
		t,
		DecoratedSignature{
			Hint:      [4]byte{27, 25, 31, 25},
			Signature: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8},
		},
		ds,
	)
}

func TestNewDecoratedSigantureForPayload_payloadLessThan4(t *testing.T) {
	ds := NewDecoratedSignatureForPayload(
		[]byte{0, 1, 2, 3, 4, 5, 6, 7, 8},
		[4]byte{9, 10, 11, 12},
		[]byte{18, 19, 20},
	)
	assert.Equal(
		t,
		DecoratedSignature{
			Hint:      [4]byte{27, 25, 31, 12},
			Signature: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8},
		},
		ds,
	)
}

package xdr

// NewDecoratedSignature creates a decorated signature using the signature and
// the key hint for the key that produced the signature.
func NewDecoratedSignature(signature []byte, keyHint [4]byte) DecoratedSignature {
	return DecoratedSignature{
		Hint:      SignatureHint(keyHint),
		Signature: Signature(signature),
	}
}

// NewDecoratedSignatureForPayload creates a decorated signature using the
// signature, the key hint for the key that produced the signature, and the
// payload that the key signed to produce the signature.
func NewDecoratedSignatureForPayload(signature []byte, keyHint [4]byte, payload []byte) DecoratedSignature {
	var payloadHint [4]byte
	if len(payload) < len(payloadHint) {
		copy(payloadHint[:], payload)
	} else {
		copy(payloadHint[:], payload[len(payload)-4:])
	}
	hint := [4]byte{
		keyHint[0] ^ payloadHint[0],
		keyHint[1] ^ payloadHint[1],
		keyHint[2] ^ payloadHint[2],
		keyHint[3] ^ payloadHint[3],
	}
	return DecoratedSignature{
		Hint:      SignatureHint(hint),
		Signature: Signature(signature),
	}
}

package configureissuer

import (
	"testing"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProduceExistingBalance(t *testing.T) {
	testAsset := txnbuild.CreditAsset{
		Code:   "FOO",
		Issuer: keypair.MustRandom().Address(),
	}

	dummyKP, gotOps, err := produceExistingBalance(testAsset)
	assert.NoError(t, err)
	assert.NotNil(t, dummyKP)

	wantOps := []txnbuild.Operation{
		&txnbuild.CreateAccount{
			Destination:   dummyKP.Address(),
			Amount:        "1.5",
			SourceAccount: testAsset.Issuer,
		},
		&txnbuild.ChangeTrust{
			Line:          testAsset,
			SourceAccount: dummyKP.Address(),
		},
		&txnbuild.AllowTrust{
			Trustor:       dummyKP.Address(),
			Authorize:     true,
			SourceAccount: testAsset.Issuer,
			Type:          testAsset,
		},
		&txnbuild.Payment{
			Destination:   dummyKP.Address(),
			Amount:        "0.0000001",
			Asset:         testAsset,
			SourceAccount: testAsset.Issuer,
		},
		&txnbuild.AllowTrust{
			Trustor:       dummyKP.Address(),
			Authorize:     false,
			SourceAccount: testAsset.Issuer,
			Type:          testAsset,
		},
	}
	require.Equal(t, wantOps, gotOps)
}

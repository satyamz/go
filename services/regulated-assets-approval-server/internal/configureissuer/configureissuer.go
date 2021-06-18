package configureissuer

import (
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/txnbuild"
)

type Options struct {
	AssetCode           string
	BaseURL             string
	HorizonURL          string
	IssuerAccountSecret string
	NetworkPassphrase   string
}

// produceExistingBalance is used to generate one trustline with a non-empty
// balance of the desired asset. This is called because many Wallets check if an
// asset exists by looking for it at `{horizon-url}/asset`.
func produceExistingBalance(asset txnbuild.CreditAsset) (dummyKP *keypair.Full, ops []txnbuild.Operation, err error) {
	dummyKP, err = keypair.Random()
	if err != nil {
		return nil, nil, errors.Wrap(err, "generating keypair")
	}

	ops = []txnbuild.Operation{
		&txnbuild.CreateAccount{
			Destination:   dummyKP.Address(),
			Amount:        "1.5",
			SourceAccount: asset.Issuer,
		},
		&txnbuild.ChangeTrust{
			Line:          asset,
			SourceAccount: dummyKP.Address(),
		},
		&txnbuild.AllowTrust{
			Trustor:       dummyKP.Address(),
			Authorize:     true,
			SourceAccount: asset.Issuer,
			Type:          asset,
		},
		&txnbuild.Payment{
			Destination:   dummyKP.Address(),
			Amount:        "0.0000001", // we're using 1 stroop just to make sure this asset will shou up at horizons /assets
			Asset:         asset,
			SourceAccount: asset.Issuer,
		},
		&txnbuild.AllowTrust{
			Trustor:       dummyKP.Address(),
			Authorize:     false,
			SourceAccount: asset.Issuer,
			Type:          asset,
		},
	}
	return dummyKP, ops, nil
}

package configureissuer

import (
	"net/http"
	"net/url"
	"time"

	"github.com/stellar/go/clients/horizonclient"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/txnbuild"
)

type Options struct {
	AssetCode           string
	BaseURL             string
	HorizonURL          string
	IssuerAccountSecret string
	NetworkPassphrase   string
}

func Setup(opts Options) {
	hClient := &horizonclient.Client{
		HorizonURL: opts.HorizonURL,
		HTTP:       &http.Client{Timeout: 30 * time.Second},
	}

	err := setup(opts, hClient)
	if err != nil {
		log.Error(errors.Wrap(err, "setting up issuer account"))
		log.Fatal("Couldn't complete setup!")
	}
}

func setup(opts Options, hClient horizonclient.ClientInterface) error {
	issuerKP, err := keypair.ParseFull(opts.IssuerAccountSecret)
	if err != nil {
		log.Fatal(errors.Wrap(err, "parsing secret"))
	}

	issuerAcc, err := hClient.AccountDetail(horizonclient.AccountRequest{
		AccountID: issuerKP.Address(),
	})
	if err != nil {
		return errors.Wrapf(err, "getting detail for account %s", issuerKP.Address())
	}
	// TODO: if account doesn't exist, fund it with friendbot if in testnet.

	asset := txnbuild.CreditAsset{
		Code:   opts.AssetCode,
		Issuer: issuerKP.Address(),
	}
	assetResults, err := hClient.Assets(horizonclient.AssetRequest{
		ForAssetCode:   asset.Code,
		ForAssetIssuer: asset.Issuer,
		Limit:          1,
	})
	if err != nil {
		return errors.Wrap(err, "getting list of assets")
	}

	u, err := url.Parse(opts.BaseURL)
	if err != nil {
		log.Fatal(err)
	}
	homeDomain := u.Hostname()

	if issuerAcc.Flags.AuthRequired && issuerAcc.Flags.AuthRevocable && issuerAcc.HomeDomain == homeDomain && len(assetResults.Embedded.Records) > 0 {
		log.Warn("Account already configured. Aborting without performing any action...")
		return nil
	}

	dummyKP, opsToProduceExistingBalance, err := produceExistingBalance(asset)
	if err != nil {
		return errors.Wrap(err, "building operations to produce existing asset trustline")
	}

	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        &issuerAcc,
		IncrementSequenceNum: true,
		Operations: append(
			[]txnbuild.Operation{
				&txnbuild.SetOptions{
					SetFlags: []txnbuild.AccountFlag{
						txnbuild.AuthRequired,
						txnbuild.AuthRevocable,
					},
					HomeDomain: &homeDomain,
				},
			},
			opsToProduceExistingBalance...,
		),
		BaseFee:    300,
		Timebounds: txnbuild.NewTimeout(300),
	})
	if err != nil {
		return errors.Wrap(err, "building transaction")
	}

	tx, err = tx.Sign(opts.NetworkPassphrase, issuerKP, dummyKP)
	if err != nil {
		return errors.Wrap(err, "signing transaction")
	}

	_, err = hClient.SubmitTransaction(tx)
	if err != nil {
		return errors.Wrap(err, "submitting transaction")
	}

	return nil
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

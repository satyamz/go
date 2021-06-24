package configureissuer

import (
	"strings"
	"testing"
	"time"

	"github.com/stellar/go/clients/horizonclient"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/txnbuild"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestSetup_accountAlreadyConfigured(t *testing.T) {
	// declare a logging buffer to validate output logs
	buf := new(strings.Builder)
	log.DefaultLogger.Logger.SetOutput(buf)
	log.DefaultLogger.Logger.SetLevel(log.InfoLevel)

	issuerKP := keypair.MustRandom()
	opts := Options{
		AssetCode:           "FOO",
		BaseURL:             "https://domain.test.com/",
		HorizonURL:          horizonclient.DefaultTestNetClient.HorizonURL,
		IssuerAccountSecret: issuerKP.Seed(),
		NetworkPassphrase:   network.TestNetworkPassphrase,
	}

	horizonMock := horizonclient.MockClient{}
	horizonMock.
		On("AccountDetail", horizonclient.AccountRequest{AccountID: issuerKP.Address()}).
		Return(horizon.Account{
			AccountID: issuerKP.Address(),
			Flags: horizon.AccountFlags{
				AuthRequired:  true,
				AuthRevocable: true,
			},
			HomeDomain: "domain.test.com",
			Sequence:   "10",
		}, nil)
	horizonMock.
		On("Assets", horizonclient.AssetRequest{
			ForAssetCode:   opts.AssetCode,
			ForAssetIssuer: issuerKP.Address(),
			Limit:          1,
		}).
		Return(horizon.AssetsPage{
			Embedded: struct{ Records []horizon.AssetStat }{
				Records: []horizon.AssetStat{
					{Amount: "0.0000001"},
				},
			},
		}, nil)

	err := setup(opts, &horizonMock)
	require.NoError(t, err)

	require.Contains(t, buf.String(), "Account already configured. Aborting without performing any action.")
}

func TestSetup(t *testing.T) {
	issuerKP := keypair.MustRandom()
	opts := Options{
		AssetCode:           "FOO",
		BaseURL:             "https://domain.test.com/",
		HorizonURL:          horizonclient.DefaultTestNetClient.HorizonURL,
		IssuerAccountSecret: issuerKP.Seed(),
		NetworkPassphrase:   network.TestNetworkPassphrase,
	}

	horizonMock := horizonclient.MockClient{}
	horizonMock.
		On("AccountDetail", horizonclient.AccountRequest{AccountID: issuerKP.Address()}).
		Return(horizon.Account{
			AccountID: issuerKP.Address(),
			Sequence:  "10",
		}, nil)
	horizonMock.
		On("Assets", horizonclient.AssetRequest{
			ForAssetCode:   opts.AssetCode,
			ForAssetIssuer: issuerKP.Address(),
			Limit:          1,
		}).
		Return(horizon.AssetsPage{}, nil)

	var didTestSubmitTransaction bool
	horizonMock.
		On("SubmitTransaction", mock.AnythingOfType("*txnbuild.Transaction")).
		Run(func(args mock.Arguments) {
			tx, ok := args.Get(0).(*txnbuild.Transaction)
			require.True(t, ok)

			issuerSimpleAcc := txnbuild.SimpleAccount{
				AccountID: issuerKP.Address(),
				Sequence:  11,
			}
			assert.Equal(t, issuerSimpleAcc, tx.SourceAccount())

			assert.Equal(t, int64(11), tx.SequenceNumber())
			assert.Equal(t, int64(300), tx.BaseFee())
			assert.Equal(t, int64(0), tx.Timebounds().MinTime)
			assert.LessOrEqual(t, time.Now().UTC().Unix()+299, tx.Timebounds().MaxTime)
			assert.GreaterOrEqual(t, time.Now().UTC().Unix()+301, tx.Timebounds().MaxTime)

			createAccOp, ok := tx.Operations()[1].(*txnbuild.CreateAccount)
			require.True(t, ok)
			dummyAccAddress := createAccOp.Destination
			homeDomain := "domain.test.com"
			testAsset := txnbuild.CreditAsset{
				Code:   opts.AssetCode,
				Issuer: issuerKP.Address(),
			}

			wantOps := []txnbuild.Operation{
				&txnbuild.SetOptions{
					SetFlags: []txnbuild.AccountFlag{
						txnbuild.AuthRequired,
						txnbuild.AuthRevocable,
					},
					HomeDomain: &homeDomain,
				},
				&txnbuild.CreateAccount{
					Destination:   dummyAccAddress,
					Amount:        "1.5",
					SourceAccount: testAsset.Issuer,
				},
				&txnbuild.ChangeTrust{
					Line:          testAsset,
					SourceAccount: dummyAccAddress,
					Limit:         "922337203685.4775807",
				},
			}
			require.Equal(t, wantOps[1:], tx.Operations()[1:])

			// SetOptions operation is validated separatedly because the value returned from tx.Operations()[0] contains the unexported field `xdrOp` that prevents a proper comparision.
			require.Equal(t, wantOps[0].(*txnbuild.SetOptions).SetFlags, tx.Operations()[0].(*txnbuild.SetOptions).SetFlags)
			require.Equal(t, wantOps[0].(*txnbuild.SetOptions).HomeDomain, tx.Operations()[0].(*txnbuild.SetOptions).HomeDomain)

			txHash, err := tx.Hash(opts.NetworkPassphrase)
			require.NoError(t, err)

			err = issuerKP.Verify(txHash[:], tx.Signatures()[0].Signature)
			require.NoError(t, err)

			dummyKp, err := keypair.ParseAddress(dummyAccAddress)
			require.NoError(t, err)
			err = dummyKp.Verify(txHash[:], tx.Signatures()[1].Signature)
			require.NoError(t, err)

			didTestSubmitTransaction = true
		}).
		Return(horizon.Transaction{}, nil)

	err := setup(opts, &horizonMock)
	require.NoError(t, err)

	require.True(t, didTestSubmitTransaction)
}

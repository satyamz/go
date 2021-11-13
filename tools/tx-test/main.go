package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/stellar/go/clients/horizonclient"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/txnbuild"
)

func main() {
	showHelp := false
	horizonURL := "http://localhost:8000"

	fs := flag.NewFlagSet("console", flag.ContinueOnError)
	fs.SetOutput(os.Stdout)
	fs.BoolVar(&showHelp, "h", showHelp, "Show this help")
	fs.StringVar(&horizonURL, "horizon", horizonURL, "Horizon URL")
	err := fs.Parse(os.Args[1:])
	if err != nil {
		panic(err)
	}
	if showHelp {
		fs.Usage()
		return
	}

	client := &horizonclient.Client{HorizonURL: horizonURL}
	networkDetails, err := client.Root()
	if err != nil {
		panic(err)
	}

	faucetSK := keypair.MustRandom()
	faucetPK := faucetSK.FromAddress()
	fmt.Fprintln(os.Stdout, "faucet account:", faucetPK.Address())
	_, err = client.Fund(faucetPK.Address())
	if horizonclient.IsNotFoundError(err) {
		fmt.Fprintf(os.Stdout, "friendbot not supported on this network\n")
		fmt.Fprintf(os.Stdout, "creating the faucet using root\n")
		rootKey := keypair.Root(networkDetails.NetworkPassphrase)
		var root horizon.Account
		root, err = client.AccountDetail(horizonclient.AccountRequest{AccountID: rootKey.Address()})
		if err != nil {
			panic(err)
		}
		rootSeq, err := root.GetSequenceNumber()
		if err != nil {
			panic(err)
		}
		var tx *txnbuild.Transaction
		tx, err = txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount:        &txnbuild.SimpleAccount{AccountID: rootKey.Address(), Sequence: rootSeq},
			IncrementSequenceNum: true,
			BaseFee:              txnbuild.MinBaseFee,
			Timebounds:           txnbuild.NewTimeout(300),
			Operations:           []txnbuild.Operation{&txnbuild.CreateAccount{Destination: faucetPK.Address(), Amount: "10000000"}},
		})
		if err != nil {
			panic(err)
		}
		tx, err = tx.Sign(networkDetails.NetworkPassphrase, rootKey)
		if err != nil {
			panic(err)
		}
		var txResp horizon.Transaction
		_, err = client.SubmitTransactionWithOptions(tx, horizonclient.SubmitTxOpts{SkipMemoRequiredCheck: true})
		if err != nil {
			panic(err)
		}
		fmt.Fprintln(os.Stdout, "faucet account created:", txResp.Successful, txResp.ResultXdr)
	}
	if err != nil {
		panic(err)
	}
	faucet, err := client.AccountDetail(horizonclient.AccountRequest{AccountID: faucetPK.Address()})
	if err != nil {
		panic(err)
	}
	faucetSeq, err := faucet.GetSequenceNumber()
	if err != nil {
		panic(err)
	}

	creatorSK := keypair.MustRandom()
	creatorPK := creatorSK.FromAddress()
	fmt.Fprintln(os.Stdout, "creator account:", creatorPK.Address())
	{
		var tx *txnbuild.Transaction
		tx, err = txnbuild.NewTransaction(txnbuild.TransactionParams{
			SourceAccount: &txnbuild.SimpleAccount{AccountID: faucetPK.Address(), Sequence: faucetSeq + 1},
			BaseFee:       txnbuild.MinBaseFee,
			Timebounds:    txnbuild.NewTimeout(300),
			Operations:    []txnbuild.Operation{&txnbuild.CreateAccount{Destination: creatorPK.Address(), Amount: "1000"}},
		})
		if err != nil {
			panic(err)
		}
		tx, err = tx.Sign(networkDetails.NetworkPassphrase, faucetSK)
		if err != nil {
			panic(err)
		}
		var txResp horizon.Transaction
		txResp, err = client.SubmitTransactionWithOptions(tx, horizonclient.SubmitTxOpts{SkipMemoRequiredCheck: true})
		if err != nil {
			panic(err)
		}
		fmt.Fprintln(os.Stdout, "creator account created:", txResp.Successful, txResp.ResultXdr)
	}
	creator, err := client.AccountDetail(horizonclient.AccountRequest{AccountID: creatorPK.Address()})
	if err != nil {
		panic(err)
	}
	creatorSeq, err := creator.GetSequenceNumber()
	if err != nil {
		panic(err)
	}

	accountSK := keypair.MustRandom()
	accountPK := accountSK.FromAddress()
	fmt.Fprintln(os.Stdout, "channel account:", accountPK.Address())
	// fmt.Fprintln(os.Stdout, "waiting before creating channel")
	// time.Sleep(5 * time.Second)

	tx, err := buildCreateAccountTx(createAccountParams{
		Creator:        creatorPK.FromAddress(),
		Account:        accountPK.FromAddress(),
		SequenceNumber: creatorSeq + 1,
		BaseFee:        0, // txnbuild.MinBaseFee,
	})
	if err != nil {
		panic(err)
	}
	tx, err = tx.Sign(networkDetails.NetworkPassphrase, creatorSK, accountSK)
	if err != nil {
		panic(err)
	}
	// txResp, err := horizonClient.SubmitTransactionWithOptions(tx, horizonclient.SubmitTxOpts{SkipMemoRequiredCheck: true})
	// if err != nil {
	// 	return fmt.Errorf("submitting tx to create channel account: %w", err)
	// }

	feeBumpTx, err := txnbuild.NewFeeBumpTransaction(txnbuild.FeeBumpTransactionParams{
		Inner:      tx,
		BaseFee:    txnbuild.MinBaseFee,
		FeeAccount: creatorPK.Address(),
	})
	if err != nil {
		panic(err)
	}
	feeBumpTx, err = feeBumpTx.Sign(networkDetails.NetworkPassphrase, creatorSK)
	if err != nil {
		panic(err)
	}
	txResp, err := client.SubmitFeeBumpTransactionWithOptions(feeBumpTx, horizonclient.SubmitTxOpts{SkipMemoRequiredCheck: true})
	if err != nil {
		panic(err)
	}

	fmt.Fprintln(os.Stdout, "channel account created:", txResp.Successful, txResp.ResultXdr)
}

type createAccountParams struct {
	Creator        *keypair.FromAddress
	Account        *keypair.FromAddress
	SequenceNumber int64
	BaseFee        int64
}

func buildCreateAccountTx(p createAccountParams) (*txnbuild.Transaction, error) {
	ops := []txnbuild.Operation{
		&txnbuild.BeginSponsoringFutureReserves{
			SponsoredID: p.Account.Address(),
		},
		&txnbuild.CreateAccount{
			Destination: p.Account.Address(),
			// base reserves sponsored by p.Creator
			Amount: "0",
		},
		&txnbuild.SetOptions{
			SourceAccount:   p.Account.Address(),
			MasterWeight:    txnbuild.NewThreshold(0),
			LowThreshold:    txnbuild.NewThreshold(1),
			MediumThreshold: txnbuild.NewThreshold(1),
			HighThreshold:   txnbuild.NewThreshold(1),
			Signer:          &txnbuild.Signer{Address: p.Creator.Address(), Weight: 1},
		},
		&txnbuild.EndSponsoringFutureReserves{
			SourceAccount: p.Account.Address(),
		},
	}

	tx, err := txnbuild.NewTransaction(
		txnbuild.TransactionParams{
			SourceAccount: &txnbuild.SimpleAccount{
				AccountID: p.Creator.Address(),
				Sequence:  p.SequenceNumber,
			},
			BaseFee:    p.BaseFee,
			Timebounds: txnbuild.NewTimeout(300),
			Operations: ops,
		},
	)
	if err != nil {
		return nil, err
	}
	return tx, nil
}

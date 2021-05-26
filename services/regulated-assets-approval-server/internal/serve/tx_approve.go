package serve

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strconv"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stellar/go/amount"
	"github.com/stellar/go/clients/horizonclient"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/services/regulated-assets-approval-server/internal/serve/httperror"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/support/http/httpdecode"
	"github.com/stellar/go/support/log"
	"github.com/stellar/go/txnbuild"
)

type txApproveHandler struct {
	issuerKP          *keypair.Full
	assetCode         string
	horizonClient     horizonclient.ClientInterface
	networkPassphrase string
	db                *sqlx.DB
	kycThreshold      int64
	baseURL           string
}

type txApproveRequest struct {
	Tx string `json:"tx" form:"tx"`
}

// validate performs some validations on the provided handler data.
func (h txApproveHandler) validate() error {
	if h.issuerKP == nil {
		return errors.New("issuer keypair cannot be nil")
	}
	if h.assetCode == "" {
		return errors.New("asset code cannot be empty")
	}
	if h.horizonClient == nil {
		return errors.New("horizon client cannot be nil")
	}
	if h.networkPassphrase == "" {
		return errors.New("network passphrase cannot be empty")
	}
	if h.db == nil {
		return errors.New("database cannot be nil")
	}
	if h.kycThreshold <= 0 {
		return errors.New("kyc threshold cannot be less than or equal to zero")
	}
	if h.baseURL == "" {
		return errors.New("base url cannot be empty")
	}
	return nil
}

// convertThresholdToReadableString parses the kycThreshold int64 value and returns a human readable string representation with precision 2 (example: 500.0000000 -> 500.00).
func convertThresholdToReadableString(threshold int64) (string, error) {
	thresholdStr := amount.StringFromInt64(threshold)
	res, err := strconv.ParseFloat(thresholdStr, 1)
	if err != nil {
		return "", errors.Wrap(err, "converting threshold amount from string to float")
	}
	return fmt.Sprintf("%.2f", res), nil
}

// checkIfTransactionOperationsAreCompliant checks if the incoming transaction operations are compliant with the expected operations in order.
func (h txApproveHandler) checkIfTransactionOperationsAreCompliant(incomingOperations []txnbuild.Operation, paymentSource string, paymentOp *txnbuild.Payment) bool {
	// AllowTrustOp only stores the AssetCode (4- or 12-char string),but does not store the issuer.
	// Since the issuer won't be in the encoded XDR we need to create a CreditAsset we should expect(which is one without an issuer).
	expectedAssetType := txnbuild.CreditAsset{
		Code:   h.assetCode,
		Issuer: "",
	}

	// Check Operation 1: AllowTrust op where issuer fully authorizes account A, asset X.
	incomingTrustOp1, isIncomingAllowTrust := incomingOperations[0].(*txnbuild.AllowTrust)
	expectedTrustOp1 := txnbuild.AllowTrust{
		Trustor:       paymentSource,
		Type:          expectedAssetType,
		Authorize:     true,
		SourceAccount: h.issuerKP.Address(),
	}
	if !isIncomingAllowTrust {
		return false
	}
	if expectedTrustOp1 != *incomingTrustOp1 {
		return false
	}

	// Check  Operation 2: AllowTrust op where issuer fully authorizes account B, asset X.
	incomingTrustOp2, isIncomingAllowTrust := incomingOperations[1].(*txnbuild.AllowTrust)
	expectedTrustOp2 := txnbuild.AllowTrust{
		Trustor:       paymentOp.Destination,
		Type:          expectedAssetType,
		Authorize:     true,
		SourceAccount: h.issuerKP.Address(),
	}
	if !isIncomingAllowTrust {
		return false
	}
	if expectedTrustOp2 != *incomingTrustOp2 {
		return false
	}

	// Check Operation 3: Payment from A to B.
	incomingPaymentOp, isIncomingPayment := incomingOperations[2].(*txnbuild.Payment)
	if !isIncomingPayment {
		return false
	}
	if incomingPaymentOp.SourceAccount == incomingPaymentOp.Destination {
		return false
	}
	if incomingPaymentOp.SourceAccount != incomingTrustOp1.Trustor {
		return false
	}
	if incomingPaymentOp.Destination != incomingTrustOp2.Trustor {
		return false
	}

	// Check Operation 4: AllowTrust op where issuer fully deauthorizes account B, asset X.
	incomingTrustOp3, isIncomingAllowTrust := incomingOperations[3].(*txnbuild.AllowTrust)
	expectedTrustOp3 := txnbuild.AllowTrust{
		Trustor:       paymentOp.Destination,
		Type:          expectedAssetType,
		Authorize:     false,
		SourceAccount: h.issuerKP.Address(),
	}
	if !isIncomingAllowTrust {
		return false
	}
	if expectedTrustOp3 != *incomingTrustOp3 {
		return false
	}

	// Check Operation 5: AllowTrust op where issuer fully deauthorizes account A, asset X.
	incomingTrustOp4, isIncomingAllowTrust := incomingOperations[4].(*txnbuild.AllowTrust)
	expectedTrustOp4 := txnbuild.AllowTrust{
		Trustor:       paymentSource,
		Type:          expectedAssetType,
		Authorize:     false,
		SourceAccount: h.issuerKP.Address(),
	}
	if !isIncomingAllowTrust {
		return false
	}
	if expectedTrustOp4 != *incomingTrustOp4 {
		return false
	}

	return true
}

func (h txApproveHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	err := h.validate()
	if err != nil {
		log.Ctx(ctx).Error(errors.Wrap(err, "validating txApproveHandler"))
		httperror.InternalServer.Render(w)
		return
	}

	in := txApproveRequest{}
	err = httpdecode.Decode(r, &in)
	if err != nil {
		log.Ctx(ctx).Error(errors.Wrap(err, "decoding txApproveRequest"))
		httperror.BadRequest.Render(w)
		return
	}

	txApproveResp, err := h.txApprove(ctx, in)
	if err != nil {
		log.Ctx(ctx).Error(errors.Wrap(err, "validating the input transaction for approval"))
		httperror.InternalServer.Render(w)
		return
	}

	txApproveResp.Render(w)
}

// validateInput performs some validations on the provided transaction. It can
// reject the transaction based on general criteria that would be applied in any
// approval server.
func (h txApproveHandler) validateInput(ctx context.Context, in txApproveRequest) (*txApprovalResponse, *txnbuild.Transaction) {
	if in.Tx == "" {
		log.Ctx(ctx).Error(`request is missing parameter "tx".`)
		return NewRejectedTxApprovalResponse(`Missing parameter "tx".`), nil
	}

	genericTx, err := txnbuild.TransactionFromXDR(in.Tx)
	if err != nil {
		log.Ctx(ctx).Error(errors.Wrap(err, "parsing transaction xdr"))
		return NewRejectedTxApprovalResponse(`Invalid parameter "tx".`), nil
	}

	tx, ok := genericTx.Transaction()
	if !ok {
		log.Ctx(ctx).Error(`invalid parameter "tx", generic transaction not given.`)
		return NewRejectedTxApprovalResponse(`Invalid parameter "tx".`), nil
	}

	if tx.SourceAccount().AccountID == h.issuerKP.Address() {
		log.Ctx(ctx).Errorf("transaction %s sourceAccount is the same as the server issuer account %s",
			in.Tx,
			h.issuerKP.Address())
		return NewRejectedTxApprovalResponse("The source account is invalid."), nil
	}

	return nil, tx
}

// checkSequenceNum checks if transaction's sequence number is equivalent to source account's sequence number+1.
func (h txApproveHandler) checkSequenceNum(ctx context.Context, tx *txnbuild.Transaction, acc horizon.Account) (*txApprovalResponse, error) {
	accountSequence, err := strconv.ParseInt(acc.Sequence, 10, 64)
	if err != nil {
		return nil, errors.Wrapf(err, "parsing account sequence number %q from string to int64", acc.Sequence)
	}
	if tx.SourceAccount().Sequence != accountSequence+1 {
		log.Ctx(ctx).Errorf(`invalid transaction sequence number tx.SourceAccount().Sequence: %d, accountSequence+1: %d`, tx.SourceAccount().Sequence, accountSequence+1)
		return NewRejectedTxApprovalResponse("Invalid transaction sequence number."), nil
	}

	return nil, nil
}

// checkIfCompliantTransaction inspects incoming transaction is compliant by wallets preemptively or by the server(according to the transaction-composition section of SEP-008).
func (h txApproveHandler) checkIfCompliantTransaction(ctx context.Context, tx *txnbuild.Transaction) (*txApprovalResponse, error) {
	// Return early if there are not 5 incoming operations to examine.
	if len(tx.Operations()) != 5 {
		return nil, nil
	}

	// Extract the payment operation, source account, and expected asset type.
	paymentOp, ok := tx.Operations()[2].(*txnbuild.Payment)
	if !ok {
		log.Ctx(ctx).Error(`third operation is not of type payment`)
		return NewRejectedTxApprovalResponse("There is one or more unauthorized operations in the provided transaction."), nil
	}
	paymentSource := paymentOp.SourceAccount
	if paymentSource == "" {
		paymentSource = tx.SourceAccount().AccountID
	}
	if paymentSource == h.issuerKP.Address() {
		log.Ctx(ctx).Error(`transaction contains one or more operations where sourceAccount is issuer account`)
		return NewRejectedTxApprovalResponse("There is one or more unauthorized operations in the provided transaction."), nil
	}

	// Compare incoming operations with expected compliant operations.
	ok = h.checkIfTransactionOperationsAreCompliant(tx.Operations(), paymentSource, paymentOp)
	if !ok {
		return NewRejectedTxApprovalResponse("There is one or more unauthorized operations in the provided transaction."), nil
	}

	// Check if sender account needs to submit KYC on the incoming transaction.
	kycRequiredResponse, err := h.handleKYCRequiredOperationIfNeeded(ctx, paymentSource, paymentOp)
	if err != nil {
		return nil, errors.Wrap(err, "handling KYC required payment")
	}
	if kycRequiredResponse != nil {
		return kycRequiredResponse, nil
	}

	// Pull current account details from the network then validate the tx sequence number.
	acc, err := h.horizonClient.AccountDetail(horizonclient.AccountRequest{AccountID: paymentSource})
	if err != nil {
		return nil, errors.Wrapf(err, "getting detail for payment source account %s", paymentSource)
	}
	txRejectedResp, err := h.checkSequenceNum(ctx, tx, acc)
	if err != nil {
		return nil, errors.Wrap(err, "checking sequence number")
	}
	if txRejectedResp != nil {
		return txRejectedResp, nil
	}

	// Sign incoming transaction with issuere's signature.
	tx, err = tx.Sign(h.networkPassphrase, h.issuerKP)
	if err != nil {
		return nil, errors.Wrap(err, "signing transaction")
	}
	txe, err := tx.Base64()
	if err != nil {
		return nil, errors.Wrap(err, "encoding revised transaction")
	}

	return NewSuccessTxApprovalResponse(txe, "Transaction is compliant and signed by the issuer."), err
}

// txApprove is called to validate the input transaction.
func (h txApproveHandler) txApprove(ctx context.Context, in txApproveRequest) (resp *txApprovalResponse, err error) {
	defer func() {
		log.Ctx(ctx).Debug("==== will log responses ====")
		log.Ctx(ctx).Debugf("req: %+v", in)
		log.Ctx(ctx).Debugf("resp: %+v", resp)
		log.Ctx(ctx).Debugf("err: %+v", err)
		log.Ctx(ctx).Debug("====  did log responses ====")
	}()

	txRejectedResp, tx := h.validateInput(ctx, in)
	if txRejectedResp != nil {
		return txRejectedResp, nil
	}

	txSuccessResp, err := h.checkIfCompliantTransaction(ctx, tx)
	if err != nil {
		return nil, errors.Wrap(err, "checking if transaction in request was compliant")
	}
	if txSuccessResp != nil {
		return txSuccessResp, nil
	}

	// Validate the revisable transaction has one operation.
	if len(tx.Operations()) != 1 {
		return NewRejectedTxApprovalResponse("Please submit a transaction with exactly one operation of type payment."), nil
	}

	// Validate payment operation.
	paymentOp, ok := tx.Operations()[0].(*txnbuild.Payment)
	if !ok {
		log.Ctx(ctx).Error(`transaction does not contain a payment operation`)
		return NewRejectedTxApprovalResponse("There is one or more unauthorized operations in the provided transaction."), nil
	}

	// Validate payment operation source account is not the issuer.
	paymentSource := paymentOp.SourceAccount
	if paymentSource == "" {
		paymentSource = tx.SourceAccount().AccountID
	}
	if paymentSource == h.issuerKP.Address() {
		log.Ctx(ctx).Error(`transaction contains one or more operations where sourceAccount is issuer account`)
		return NewRejectedTxApprovalResponse("There is one or more unauthorized operations in the provided transaction."), nil
	}

	// Validate payment operation is supported by the issuer.
	issuerAddress := h.issuerKP.Address()
	if paymentOp.Asset.GetCode() != h.assetCode || paymentOp.Asset.GetIssuer() != issuerAddress {
		log.Ctx(ctx).Error(`the payment asset is not supported by this issuer`)
		return NewRejectedTxApprovalResponse("The payment asset is not supported by this issuer."), nil
	}

	// Pull current account details from the network.
	acc, err := h.horizonClient.AccountDetail(horizonclient.AccountRequest{AccountID: paymentSource})
	if err != nil {
		return nil, errors.Wrapf(err, "getting detail for payment source account %s", paymentSource)
	}
	// Validate the sequence number.
	txRejectedResp, err = h.checkSequenceNum(ctx, tx, acc)
	if err != nil {
		return nil, errors.Wrap(err, "checking sequence number")
	}
	if txRejectedResp != nil {
		return txRejectedResp, nil
	}

	// Validate if payment operation requires KYC.
	var kycRequiredResponse *txApprovalResponse
	kycRequiredResponse, err = h.handleKYCRequiredOperationIfNeeded(ctx, paymentSource, paymentOp)
	if err != nil {
		return nil, errors.Wrap(err, "handling KYC required payment")
	}
	if kycRequiredResponse != nil {
		return kycRequiredResponse, nil
	}
	// build the transaction
	revisedOperations := []txnbuild.Operation{
		&txnbuild.AllowTrust{
			Trustor:       paymentSource,
			Type:          paymentOp.Asset,
			Authorize:     true,
			SourceAccount: issuerAddress,
		},
		&txnbuild.AllowTrust{
			Trustor:       paymentOp.Destination,
			Type:          paymentOp.Asset,
			Authorize:     true,
			SourceAccount: issuerAddress,
		},
		paymentOp,
		&txnbuild.AllowTrust{
			Trustor:       paymentOp.Destination,
			Type:          paymentOp.Asset,
			Authorize:     false,
			SourceAccount: issuerAddress,
		},
		&txnbuild.AllowTrust{
			Trustor:       paymentSource,
			Type:          paymentOp.Asset,
			Authorize:     false,
			SourceAccount: issuerAddress,
		},
	}
	revisedTx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        &acc,
		IncrementSequenceNum: true,
		Operations:           revisedOperations,
		BaseFee:              300,
		Timebounds:           txnbuild.NewTimeout(300),
	})
	if err != nil {
		return nil, errors.Wrap(err, "building transaction")
	}

	revisedTx, err = revisedTx.Sign(h.networkPassphrase, h.issuerKP)
	if err != nil {
		return nil, errors.Wrap(err, "signing transaction")
	}

	txe, err := revisedTx.Base64()
	if err != nil {
		return nil, errors.Wrap(err, "encoding revised transaction")
	}

	return NewRevisedTxApprovalResponse(txe), nil
}

// handleKYCRequiredOperationIfNeeded validates and returns an action_required response if the payment requires KYC.
func (h txApproveHandler) handleKYCRequiredOperationIfNeeded(ctx context.Context, stellarAddress string, paymentOp *txnbuild.Payment) (*txApprovalResponse, error) {
	// validate payment operation against KYC condition(s).
	KYCRequiredMessage, err := h.kycRequiredMessageIfNeeded(paymentOp)
	if err != nil {
		return nil, errors.Wrap(err, "validating KYC")
	}
	if KYCRequiredMessage == "" {
		return nil, nil
	}

	intendedCallbackID := uuid.New().String()
	const q = `
		WITH new_row AS (
			INSERT INTO accounts_kyc_status (stellar_address, callback_id)
			VALUES ($1, $2)
			ON CONFLICT(stellar_address) DO NOTHING
			RETURNING *
		)
		SELECT callback_id, approved_at, rejected_at FROM new_row
		UNION
		SELECT callback_id, approved_at, rejected_at
		FROM accounts_kyc_status
		WHERE stellar_address = $1
	`
	var (
		callbackID             string
		approvedAt, rejectedAt sql.NullTime
	)
	err = h.db.QueryRowContext(ctx, q, stellarAddress, intendedCallbackID).Scan(&callbackID, &approvedAt, &rejectedAt)
	if err != nil {
		return nil, errors.Wrap(err, "inserting new row into accounts_kyc_status table")
	}

	if approvedAt.Valid {
		return nil, nil
	}
	if rejectedAt.Valid {
		kycThreshold, err := convertThresholdToReadableString(h.kycThreshold)
		if err != nil {
			return nil, errors.Wrap(err, "converting kycThreshold to human readable string")
		}
		return NewRejectedTxApprovalResponse(fmt.Sprintf("Your KYC was rejected and you're not authorized for operations above %s %s.", kycThreshold, h.assetCode)), nil
	}

	return NewActionRequiredTxApprovalResponse(
		KYCRequiredMessage,
		fmt.Sprintf("%s/kyc-status/%s", h.baseURL, callbackID),
		[]string{"email_address"},
	), nil
}

// kycRequiredMessageIfNeeded returns a "action_required" message for the NewActionRequiredTxApprovalResponse if the payment operation meets KYC conditions.
// Currently rule(s) are, checking if payment amount is > KYCThreshold amount.
func (h txApproveHandler) kycRequiredMessageIfNeeded(paymentOp *txnbuild.Payment) (string, error) {
	paymentAmount, err := amount.ParseInt64(paymentOp.Amount)
	if err != nil {
		return "", errors.Wrap(err, "parsing account payment amount from string to Int64")
	}
	if paymentAmount > h.kycThreshold {
		kycThreshold, err := convertThresholdToReadableString(h.kycThreshold)
		if err != nil {
			return "", errors.Wrap(err, "converting kycThreshold to human readable string")
		}
		return fmt.Sprintf(`Payments exceeding %s %s requires KYC approval. Please provide an email address.`, kycThreshold, h.assetCode), nil
	}
	return "", nil
}

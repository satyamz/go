package orderbook

import (
	"math"

	"github.com/holiman/uint256"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/xdr"
)

// There are two different exchanges that can be simulated:
//
// 1. You know how much you can *give* to the pool, and are curious about the
// resulting payout. We call this a "deposit", and you should pass
// tradeTypeDeposit.
//
// 2. You know how much you'd like to *receive* from the pool, and want to know
// how much to deposit to achieve this. We call this an "expectation", and you
// should pass tradeTypeExpectation.
const (
	tradeTypeDeposit     = iota // deposit into pool, what's the payout?
	tradeTypeExpectation = iota // expect payout, what to deposit?
)

var (
	errPoolOverflows = errors.New("Liquidity pool overflows from this exchange")
	errBadPoolType   = errors.New("Unsupported liquidity pool: must be ConstantProduct")
	errBadTradeType  = errors.New("Unknown pool exchange type requested")
	errBadAmount     = errors.New("Exchange amount must be positive")
)

// makeTrade simulates execution of an exchange with a liquidity pool.
//
// In (1), this returns the amount that would be paid out by the pool (in terms
// of the *other* asset) for depositing `amount` of `asset`.
//
// In (2), this returns the amount of `asset` you'd need to deposit to get
// `amount` of the *other* asset in return.
//
// Refer to https://github.com/stellar/stellar-protocol/blob/master/core/cap-0038.md#pathpaymentstrictsendop-and-pathpaymentstrictreceiveop
// and the calculation functions (below) for details on the exchange algorithm.
//
// Warning: If you pass an asset that is NOT one of the pool reserves, the
// behavior of this function is undefined (for performance).
func makeTrade(
	pool liquidityPool,
	asset int32,
	tradeType int,
	amount xdr.Int64,
) (xdr.Int64, error) {
	details, ok := pool.Body.GetConstantProduct()
	if !ok {
		return 0, errBadPoolType
	}

	if amount <= 0 {
		return 0, errBadAmount
	}

	// determine which asset `amount` corresponds to
	X, Y := details.ReserveA, details.ReserveB
	if pool.assetA != asset {
		X, Y = Y, X
	}

	ok = false
	var result xdr.Int64
	switch tradeType {
	case tradeTypeDeposit:
		result, _, ok = CalculatePoolPayout(X, Y, amount, details.Params.Fee, false)

	case tradeTypeExpectation:
		result, ok = calculatePoolExpectation(X, Y, amount, details.Params.Fee)

	default:
		return 0, errBadTradeType
	}

	if !ok {
		// the error isn't strictly accurate (e.g. it could be div-by-0), but
		// from the caller's perspective it's true enough
		return 0, errPoolOverflows
	}
	return result, nil
}

// We do all of the math with 4 extra decimal places of precision, so it's
// all upscaled by 10_000.
var centibips = uint256.NewInt(10_000)
var bips = uint256.NewInt(100)

// CalculatePoolPayout calculates the amount of `reserveB` disbursed from the
// pool for a `received` amount of `reserveA` . From CAP-38:
//
//      y = floor[(1 - F) Yx / (X + x - Fx)]
//
// It returns false if the calculation overflows.
func CalculatePoolPayout(reserveA, reserveB, received xdr.Int64, feeBips xdr.Int32, calculateRoundingSlippage bool) (xdr.Int64, xdr.Int64, bool) {
	X, Y := uint256.NewInt(uint64(reserveA)), uint256.NewInt(uint64(reserveB))
	F, x := uint256.NewInt(uint64(feeBips)), uint256.NewInt(uint64(received))

	// would this deposit overflow the reserve?
	if received > math.MaxInt64-reserveA {
		return 0, 0, false
	}

	f := new(uint256.Int).Sub(centibips, F) // upscaled 1 - F

	// right half: X + (1 - F)x
	denom := X.Mul(X, centibips).Add(X, new(uint256.Int).Mul(x, f))
	if denom.IsZero() { // avoid div-by-zero panic
		return 0, 0, false
	}

	// left half, a: (1 - F) Yx
	numer := Y.Mul(Y, x).Mul(Y, f)

	// divide & check overflow
	result := new(uint256.Int)
	result.Div(numer, denom)

	var roundingSlippageBips xdr.Int64
	ok := true
	if calculateRoundingSlippage && !new(uint256.Int).Mod(numer, denom).IsZero() {
		S := new(uint256.Int) // Rounding Slippage in bips
		// Recalculate with more precision
		unrounded, rounded := new(uint256.Int), new(uint256.Int)
		unrounded.Mul(numer, centibips).Div(unrounded, denom)
		rounded.Mul(result, centibips)
		S.Sub(unrounded, rounded)
		S.Abs(S).Mul(S, centibips)
		S.Div(S, unrounded)
		S.Div(S, bips) // Take off the excess 2 decimal places
		roundingSlippageBips = xdr.Int64(S.Uint64())
		ok = ok && S.IsUint64() && roundingSlippageBips >= 0
	}

	val := xdr.Int64(result.Uint64())
	ok = ok && result.IsUint64() && val >= 0
	return val, roundingSlippageBips, ok
}

// calculatePoolExpectation determines how much of `reserveA` you would need to
// put into a pool to get the `disbursed` amount of `reserveB`.
//
//      x = ceil[Xy / ((Y - y)(1 - F))]
//
// It returns false if the calculation overflows.
func calculatePoolExpectation(
	reserveA, reserveB, disbursed xdr.Int64, feeBips xdr.Int32,
) (xdr.Int64, bool) {
	result, _, ok := poolExpectationCentibips(reserveA, reserveB, disbursed, feeBips)
	if !ok {
		return 0, false
	}
	// Downscale back to stroops
	result.Div(result, centibips)

	val := xdr.Int64(result.Uint64())
	return val, result.IsUint64() && val >= 0
}

// CalculatePoolExpectationRoundingSlippage calculates the rounding slippage (S) in bips (Basis points)
//
// S is the % which the rounded result deviates from the unrounded.
// i.e. How much "error" did the rounding introduce?
//
//      unrounded = Xy / ((Y - y)(1 - F))
//      expectation = ceil[unrounded]
//      S = abs(expectation - unrounded) / unrounded
//
// For example, for:
//
//      X = 200    // 200 stroops of deposited asset in reserves
//      Y = 300    // 300 stroops of disbursed asset in reserves
//      y = 3      // disbursing 3 stroops
//      F = 0.003  // fee is 0.3%
//      unrounded = (200 * 3) / ((300 - 3)(1 - 0.003)) = 2.03
//      S = abs(ceil(2.03) - 2.03) / 2.03 = 47.78%
//      toBips(S) = 4778
//
func CalculatePoolExpectationRoundingSlippage(
	reserveA, reserveB, disbursed xdr.Int64, feeBips xdr.Int32,
) (xdr.Int64, bool) {
	rounded, rem, ok := poolExpectationCentibips(reserveA, reserveB, disbursed, feeBips)
	if !ok {
		return 0, false
	}

	if rem.IsZero() {
		return 0, true
	}

	unrounded := rounded.Clone()
	unrounded.Sub(unrounded, centibips).Add(unrounded, rem)

	// S = abs(ceil(unrounded) - unrounded) / unrounded
	//
	// we can simplify the numerator as:
	// abs(ceil(unrounded) - unrounded) = 1 - rem
	//
	// and, since we are working in centibips, 1 = centibip
	//
	// But then we have to downscale at the end to get back to bips.
	S := new(uint256.Int).Sub(centibips, rem) // S == 1 - rem
	S.Mul(S, centibips)                       // upscale
	S.Div(S, unrounded)                       // S / unrounded
	S.Div(S, bips)                            // downscale to bips
	roundingSlippageBips := xdr.Int64(S.Uint64())
	return roundingSlippageBips, S.IsUint64() && roundingSlippageBips >= 0
}

// poolExpectationCentibips determines how much of `reserveA` you
// would need to put into a pool to get the `disbursed` amount of `reserveB`.
// This intermediate version upscales the result to include 4 extra decimals of
// precision.
//
//      x = 10_000 * ceil[Xy / ((Y - y)(1 - F))]
//
// It returns false if the calculation overflows.
func poolExpectationCentibips(
	reserveA, reserveB, disbursed xdr.Int64, feeBips xdr.Int32,
) (*uint256.Int, *uint256.Int, bool) {
	X, Y := uint256.NewInt(uint64(reserveA)), uint256.NewInt(uint64(reserveB))
	F, y := uint256.NewInt(uint64(feeBips)), uint256.NewInt(uint64(disbursed))

	// sanity check: disbursing shouldn't underflow the reserve
	if disbursed >= reserveB {
		return nil, nil, false
	}

	f := new(uint256.Int).Sub(centibips, F) // upscaled 1 - F

	denom := Y.Sub(Y, y).Mul(Y, f) // right half: (Y - y)(1 - F)
	if denom.IsZero() {            // avoid div-by-zero panic
		return nil, nil, false
	}

	numer := X.Mul(X, y).Mul(X, centibips) // left half: Xy

	// Upscale, then divide
	result := new(uint256.Int).Div(numer.Mul(numer, centibips), denom)
	rem := new(uint256.Int).Mod(result, centibips)

	// hacky way to ceil(): if there's a remainder, add 1
	if !rem.IsZero() {
		result.Add(result, centibips).Sub(result, rem)
	}

	return result, rem, true
}

// getOtherAsset returns the other asset in the liquidity pool. Note that
// doesn't check to make sure the passed in `asset` is actually part of the
// pool; behavior in that case is undefined.
func getOtherAsset(asset int32, pool liquidityPool) int32 {
	if pool.assetA == asset {
		return pool.assetB
	}
	return pool.assetA
}

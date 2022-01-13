package db

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNullRatSerialization_Null(t *testing.T) {
	nr := NewNullRat(nil, false)
	v, err := nr.Value()
	require.NoError(t, err)
	require.Nil(t, v)
	var result NullRat
	require.NoError(t, result.Scan(v))
	require.False(t, result.Valid)
	require.Nil(t, result.Rat)
}

func TestNullRatSerialization_One(t *testing.T) {
	one := big.NewRat(1, 1)
	nr := NewNullRat(one, true)
	v, err := nr.Value()
	require.NoError(t, err)
	require.NotNil(t, v)
	var result NullRat
	require.NoError(t, result.Scan(v))
	require.True(t, result.Valid)
	require.Equal(t, one, result.Rat)
}

func TestNullRatSerialization_Repeating(t *testing.T) {
	oneThird := big.NewRat(1, 3)
	nr := NewNullRat(oneThird, true)
	v, err := nr.Value()
	require.NoError(t, err)
	require.Equal(t, "0.333333333333333333", v)
	var result NullRat
	require.NoError(t, result.Scan(v))
	require.True(t, result.Valid)

	// for now it gets truncated to 18 decimals
	require.Equal(t, oneThird.FloatString(18), result.Rat.FloatString(18))
}

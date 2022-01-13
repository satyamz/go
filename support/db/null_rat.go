package db

import (
	"database/sql/driver"
	"fmt"
	"math/big"
)

// NullRat represents a math/big.Rat that may be null.
// NullRat implements the Scanner interface so
// it can be used as a scan destination:
//
//  var r NullRat
//  err := db.QueryRow("SELECT big_ratio FROM foo WHERE id=?", id).Scan(&r)
//  ...
//  if r.Valid {
//     // use r.Rat
//  } else {
//     // NULL value
//  }
type NullRat struct {
	Rat   *big.Rat
	Valid bool
}

// NewNullRat constructs a new NullRat
func NewNullRat(r *big.Rat, valid bool) NullRat {
	return NullRat{
		Rat:   r,
		Valid: valid,
	}
}

// Scan implements the Scanner interface.
func (nr *NullRat) Scan(value interface{}) error {
	switch v := value.(type) {
	case int64:
		nr.Rat = new(big.Rat).SetInt64(v)
		nr.Valid = true
		return nil
	case float64:
		nr.Rat = new(big.Rat).SetFloat64(v)
		nr.Valid = true
		return nil
	case []byte:
		r, ok := new(big.Rat).SetString(string(v))
		if !ok {
			return fmt.Errorf("invalid rational: %q", string(v))
		}
		nr.Rat = r
		nr.Valid = true
		return nil
	case string:
		r, ok := new(big.Rat).SetString(v)
		if !ok {
			return fmt.Errorf("invalid rational: %q", v)
		}
		nr.Rat = r
		nr.Valid = true
		return nil
	case nil:
		nr.Rat, nr.Valid = nil, false
		return nil
	default:
		return fmt.Errorf("unsupported Scan, storing driver.Value type %T into type %T", value, nr.Rat)
	}
}

// Value implements the driver Valuer interface.
func (nr NullRat) Value() (driver.Value, error) {
	if !nr.Valid {
		return nil, nil
	}
	// 18 is arbitrary here.
	return nr.Rat.FloatString(18), nil
}

func (nr NullRat) Equal(other interface{}) bool {
	if !nr.Valid {
		switch o := other.(type) {
		case nil:
			return true
		case NullRat:
			return !o.Valid
		default:
			return false
		}
	}

	switch o := other.(type) {
	case NullRat:
		return o.Valid && nr.Rat.Cmp(o.Rat) == 0
	case *big.Rat:
		return nr.Rat.Cmp(o) == 0
	case big.Rat:
		return nr.Rat.Cmp(&o) == 0
	default:
		return false
	}
}

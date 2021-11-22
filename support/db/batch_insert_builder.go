package db

import (
	"context"
	"fmt"
	"reflect"
	"sort"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/stellar/go/support/errors"
)

// BatchInsertBuilder works like sq.InsertBuilder but has a better support for batching
// large number of rows.
// It is NOT safe for concurrent use.
type BatchInsertBuilder struct {
	Table *Table
	// TODO: now unused
	MaxBatchSize int

	// Suffix adds a sql expression to the end of the query (e.g. an ON CONFLICT clause)
	Suffix        string
	stmt          *sqlx.Stmt
	columns       []string
	rowStructType reflect.Type
}

// Row adds a new row to the batch. All rows must have exactly the same columns
// (map keys). Otherwise, error will be returned. Please note that rows are not
// added one by one but in batches when `Exec` is called (or `MaxBatchSize` is
// reached).
func (b *BatchInsertBuilder) Row(ctx context.Context, row map[string]interface{}) error {
	if b.columns == nil {
		b.columns = make([]string, 0, len(row))

		for column := range row {
			b.columns = append(b.columns, column)
		}

		sort.Strings(b.columns)

		if err := b.initStmt(ctx); err != nil {
			return err
		}
	}

	if len(b.columns) != len(row) {
		return errors.Errorf("invalid number of columns (expected=%d, actual=%d)", len(b.columns), len(row))
	}

	rowSlice := make([]interface{}, 0, len(b.columns))
	for _, column := range b.columns {
		val, ok := row[column]
		if !ok {
			return errors.Errorf(`column "%s" does not exist`, column)
		}
		rowSlice = append(rowSlice, val)
	}

	_, err := b.stmt.ExecContext(ctx, rowSlice...)
	return err
}

func (b *BatchInsertBuilder) initStmt(ctx context.Context) error {
	// TODO: could the transaction had been started before?
	if err := b.Table.Session.Begin(); err != nil {
		return err
	}
	_, err := b.Table.Session.GetTx().ExecContext(
		ctx,
		fmt.Sprintf("CREATE TEMP TABLE tmp_table (LIKE %s INCLUDING DEFAULTS) ON COMMIT DROP", b.Table.Name),
	)
	if err != nil {
		return err
	}
	stmt, err := b.Table.Session.GetTx().PreparexContext(ctx, pq.CopyIn("tmp_table", b.columns...))
	if err != nil {
		return err
	}
	b.stmt = stmt
	return nil
}

func (b *BatchInsertBuilder) RowStruct(ctx context.Context, row interface{}) error {
	if b.columns == nil {
		b.columns = ColumnsForStruct(row)
		if err := b.initStmt(ctx); err != nil {
			return err
		}
	}

	rowType := reflect.TypeOf(row)
	if b.rowStructType == nil {
		b.rowStructType = rowType
	} else if b.rowStructType != rowType {
		return errors.Errorf(`expected value of type "%s" but got "%s" value`, b.rowStructType.String(), rowType.String())
	}

	rrow := reflect.ValueOf(row)
	rvals := mapper.FieldsByName(rrow, b.columns)

	// convert fields values to interface{}
	columnValues := make([]interface{}, len(b.columns))
	for i, rval := range rvals {
		columnValues[i] = rval.Interface()
	}
	if len(columnValues) == 0 {
		// otherwise the exec below would result in a the statement being closed
		return nil
	}

	_, err := b.stmt.ExecContext(ctx, columnValues...)
	return err
}

// Exec inserts rows in batches. In case of errors it's possible that some batches
// were added so this should be run in a DB transaction for easy rollbacks.
func (b *BatchInsertBuilder) Exec(ctx context.Context) error {
	if b.stmt == nil {
		return nil
	}
	if _, err := b.stmt.ExecContext(ctx); err != nil {
		return err
	}
	if err := b.stmt.Close(); err != nil {
		return err
	}
	_, err := b.Table.Session.GetTx().ExecContext(
		ctx,
		fmt.Sprintf("INSERT INTO %s SELECT * FROM tmp_table %s", b.Table.Name, b.Suffix),
	)
	if err != nil {
		return err
	}
	return b.Table.Session.Commit()
}

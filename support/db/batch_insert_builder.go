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
	columns       []string
	rows          [][]interface{}
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

	b.rows = append(b.rows, rowSlice)

	return nil
}

func (b *BatchInsertBuilder) RowStruct(ctx context.Context, row interface{}) error {
	if b.columns == nil {
		b.columns = ColumnsForStruct(row)
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
	b.rows = append(b.rows, columnValues)
	return nil
}

// Exec inserts rows in batches. In case of errors it's possible that some batches
// were added so this should be run in a DB transaction for easy rollbacks.
func (b *BatchInsertBuilder) Exec(ctx context.Context) (err error) {
	if len(b.rows) == 0 {
		// Nothing to do
		return nil
	}
	var (
		bookKeepTx bool
		stmt       *sqlx.Stmt
	)

	// cleanup
	defer func() {
		if stmt != nil {
			stmt.Close()
		}
		if bookKeepTx && b.Table.Session.GetTx() != nil {
			b.Table.Session.Rollback()
		}
	}()

	// Begin a transaction if it wasn't started externally
	if b.Table.Session.GetTx() == nil {
		if err := b.Table.Session.Begin(); err != nil {
			return err
		}
		bookKeepTx = true
	}

	// Ensure there is temporary table were to COPY the content
	// and later merge into the final table (needed to support the insert suffix)
	_, err = b.Table.Session.GetTx().ExecContext(
		ctx,
		fmt.Sprintf("CREATE TEMP TABLE IF NOT EXISTS tmp_%s (LIKE %s INCLUDING DEFAULTS) ON COMMIT DROP", b.Table.Name, b.Table.Name),
	)
	if err != nil {
		return
	}

	// Start COPY
	stmt, err = b.Table.Session.GetTx().PreparexContext(ctx, pq.CopyIn("tmp_"+b.Table.Name, b.columns...))
	if err != nil {
		return
	}

	// COPY values into temporary table
	for _, r := range b.rows {
		if _, err = stmt.ExecContext(ctx, r...); err != nil {
			return
		}

	}
	if _, err = stmt.ExecContext(ctx); err != nil {
		// wrap up statement execution
		return
	}

	err = stmt.Close()
	// mark statement as closed
	stmt = nil
	if err != nil {
		return
	}

	// Merge temporary table with final table, using insertion Suffix
	_, err = b.Table.Session.GetTx().ExecContext(
		ctx,
		fmt.Sprintf("INSERT INTO %s SELECT * FROM tmp_%s %s", b.Table.Name, b.Table.Name, b.Suffix),
	)
	if err != nil {
		return
	}

	// Truncate temporary table
	// TODO: we could avoid this if we have guarantees of Exec() only being called once
	//       per transaction
	_, err = b.Table.Session.GetTx().ExecContext(
		ctx,
		fmt.Sprintf("TRUNCATE TABLE tmp_%s", b.Table.Name),
	)
	if err != nil {
		return
	}

	if bookKeepTx {
		err = b.Table.Session.Commit()
	}
	if err == nil {
		// Clear the rows so user can reuse it for batch inserting to a single table
		b.rows = make([][]interface{}, 0)
	}
	return
}

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/lib/pq"

	"github.com/stellar/go/support/db"
)

const header = `
SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

-- Set these in postgres.conf
-- maintenance_work_mem = 512MB
-- max_wal_size = 4GB
-- archive_mode = off
-- wal_level = minimal
-- max_wal_senders = 0

SET default_tablespace = '';

-- SET default_table_access_method = heap;
`

func main() {
	dbUrl := flag.String("db-url", "", "Database Url")
	table := flag.String("table", "", "Set this to limit to a single table")
	multiplier := flag.Int("multiplier", 0, "How many times to multiply the database size")
	rows := flag.Int("rows", 0, "Limit the number of inserted rows per table, for quicker testing runs")
	perTxn := flag.Int("per-txn", 100000, "How many max to insert in one txn")
	flag.Parse()

	if *dbUrl == "" {
		log.Fatal("--db-url is required")
	}

	if *rows == 0 && *multiplier == 0 {
		log.Fatal("Either --rows or --multiplier are required")
	} else if *rows > 0 && *multiplier > 0 {
		log.Fatal("Cannot set both --rows and --multiplier")
	}

	log.Println("Calculating work...")
	jobsByTable := map[string][]Job{}
	var total uint64
	for _, t := range tables {
		if *table != "" && t.Name != *table {
			log.Println(t.Name, "(skip)")
			continue
		}
		jobs, nRecords, err := t.Jobs(*dbUrl, *multiplier, *rows, *perTxn)
		if err != nil {
			log.Fatal(err)
		}
		jobsByTable[t.Name] = jobs
		total += nRecords
		log.Printf("%s (ready) jobs: %d, records: %d\n", t.Name, len(jobs), nRecords)
	}
	log.Println("Total Records To Insert:", total)

	start := time.Now()
	var totalInserted uint64
	for _, t := range tables {
		jobs, ok := jobsByTable[t.Name]
		if !ok || len(jobs) == 0 {
			continue
		}

		steps := len(jobs) + 2

		log.Printf("%s (%d/%d) before\n", t.Name, 1, steps)
		if t.Before != nil {
			session, err := db.Open("postgres", *dbUrl)
			if err != nil {
				log.Fatal(err)
			}
			if err := t.Before(context.Background(), session); err != nil {
				session.Close()
				log.Fatal(err)
			}
			session.Close()
		}

		tableStart := time.Now()
		var inserted uint64
		for i, batch := range jobs {
			log.Printf("%s (batch %d/%d) started\n", t.Name, i+2, steps)
			batchStart := time.Now()
			nInserted, err := batch(context.Background())
			inserted += nInserted
			totalInserted += nInserted
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("%s (%d/%d) batch inserted: %d, duration: %v\n", t.Name, i+2, steps, nInserted, time.Since(batchStart))
			log.Printf("progress: %.2f%%, duration: %v\n", 100*float64(totalInserted)/float64(total), time.Since(start))
		}

		log.Printf("%s (%d/%d) after\n", t.Name, steps, steps)
		if t.After != nil {
			session, err := db.Open("postgres", *dbUrl)
			if err != nil {
				log.Fatal(err)
			}
			if err := t.After(context.Background(), session); err != nil {
				session.Close()
				log.Fatal(err)
			}
			session.Close()
		}

		log.Printf("%s (%d/%d) done, inserted: %d, duration: %v\n", t.Name, steps, steps, inserted, time.Since(tableStart))
	}
	log.Printf("(done) total: %d, duration: %v\n", totalInserted, time.Since(start))
}

type Job = func(ctx context.Context) (uint64, error)

var base32Alphabet = []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ234567")

func randomString(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = base32Alphabet[rand.Intn(len(base32Alphabet))]
	}
	return string(b)
}

func randomAddress() string {
	return randomString(56)
}

var tables = []Table{
	{
		Name:    "history_accounts",
		Columns: []string{"id", "address"},
		Generate: func(id uint64) ([]interface{}, error) {
			return []interface{}{id, randomAddress()}, nil
		},
		Before: func(ctx context.Context, session db.SessionInterface) error {
			_, err := session.ExecRaw(ctx, `
				-- ALTER TABLE public.history_trades DROP CONSTRAINT IF EXISTS "history_trades_base_account_id_fkey";
				-- ALTER TABLE public.history_trades DROP CONSTRAINT IF EXISTS "history_trades_counter_account_id_fkey";
				-- DROP INDEX IF EXISTS public.index_history_accounts_on_address;
				-- DROP INDEX IF EXISTS public.index_history_accounts_on_id;
			`)
			return err
		},
		After: func(ctx context.Context, session db.SessionInterface) error {
			_, err := session.ExecRaw(ctx, `
				-- TODO
				-- CREATE UNIQUE INDEX index_history_accounts_on_address ON public.history_accounts USING btree (address);
				-- CREATE UNIQUE INDEX index_history_accounts_on_id ON public.history_accounts USING btree (id);
				-- ALTER TABLE public.history_trades ADD CONSTRAINT "history_trades_base_account_id_fkey" FOREIGN KEY (base_account_id) REFERENCES history_accounts(id);
				-- ALTER TABLE public.history_trades ADD CONSTRAINT "history_trades_counter_account_id_fkey" FOREIGN KEY (counter_account_id) REFERENCES history_accounts(id);
			`)
			return err
		},
	},
	{
		Name: "history_transactions",
		Columns: []string{
			"id",
			"transaction_hash",
			"ledger_sequence",
			"application_order",
			"account",
			"account_sequence",
			"max_fee",
			"operation_count",
			"created_at",
			"updated_at",
			"tx_envelope",
			"tx_result",
			"tx_meta",
			"tx_fee_meta",
			"signatures",
			"memo_type",
			"memo",
			// "time_bounds", // TODO: Ignoring this for now.
			"successful",
			"fee_charged",
			"inner_transaction_hash",
			"fee_account",
			"inner_signatures",
			"new_max_fee",
			"account_muxed",
			"fee_account_muxed",
		},
		Generate: func(id uint64) ([]interface{}, error) {
			// These string sizes are based on this (run 2021-07-30):
			// SELECT avg(transaction_hash) as transaction_hash, avg(account) as account, avg(tx_envelope) as tx_envelope, avg(tx_result) as tx_result, avg(tx_meta) as tx_meta, avg(tx_fee_meta) as tx_fee_meta, avg(signatures) as signatures, avg(memo_type) as memo_type, avg(memo) as memo, avg(inner_transaction_hash) as inner_transaction_hash, avg(fee_account) as fee_account, avg(inner_signatures) as inner_signatures, avg(account_muxed) as account_muxed, avg(fee_account_muxed) as fee_account_muxed FROM ( select pg_column_size(transaction_hash) as transaction_hash, pg_column_size(account) as account, pg_column_size(tx_envelope) as tx_envelope, pg_column_size(tx_result) as tx_result, pg_column_size(tx_meta) as tx_meta, pg_column_size(tx_fee_meta) as tx_fee_meta, pg_column_size(signatures) as signatures, pg_column_size(memo_type) as memo_type, pg_column_size(memo) as memo, pg_column_size(inner_transaction_hash) as inner_transaction_hash, pg_column_size(fee_account) as fee_account, pg_column_size(inner_signatures) as inner_signatures, pg_column_size(account_muxed) as account_muxed, pg_column_size(fee_account_muxed) as fee_account_muxed FROM history_transactions) as ht;
			// -[ RECORD 1 ]----------+---------------------
			// transaction_hash       | 65.0000000000000000
			// account                | 57.0000000000000000
			// tx_envelope            | 407.1012360000000000
			// tx_result              | 220.9261980000000000
			// tx_meta                | 729.8140260000000000
			// tx_fee_meta            | 284.4806350000000000
			// signatures             | 123.3363870000000000
			// memo_type              | 4.9112300000000000
			// memo                   | 14.5654434288085330
			// inner_transaction_hash | 65.0000000000000000
			// fee_account            | 57.0000000000000000
			// inner_signatures       | 293.8940533151059467
			// account_muxed          |
			// fee_account_muxed      |

			return []interface{}{
				id,
				randomString(64),  // transaction_hash       | character varying(64)       |           | not null |
				1,                 // ledger_sequence        | integer                     |           | not null |
				0,                 // application_order      | integer                     |           | not null |
				randomAddress(),   // account                | character varying(64)       |           | not null |
				0,                 // account_sequence       | bigint                      |           | not null |
				0,                 // max_fee                | bigint                      |           | not null |
				0,                 // operation_count        | integer                     |           | not null |
				time.Now(),        // created_at             | timestamp without time zone |           |          |
				time.Now(),        // updated_at             | timestamp without time zone |           |          |
				randomString(406), // tx_envelope            | text                        |           | not null |
				randomString(219), // tx_result              | text                        |           | not null |
				randomString(728), // tx_meta                | text                        |           | not null |
				randomString(283), // tx_fee_meta            | text                        |           | not null |
				pq.StringArray([]string{randomString(61), randomString(61)}), // signatures             | character varying(96)[]     |           | not null | '{}'::character varying[]
				randomString(3),  // memo_type              | character varying           |           | not null | 'none'::character varying
				randomString(13), // memo                   | character varying           |           |          |
				// []byte{0, 0},     // time_bounds            | int8range                   |           |          |
				true,             // successful             | boolean                     |           |          |
				1,                // fee_charged            | bigint                      |           |          |
				randomString(64), // inner_transaction_hash | character varying(64)       |           |          |
				randomAddress(),  // fee_account            | character varying(64)       |           |          |
				pq.StringArray([]string{randomString(73), randomString(73), randomString(73), randomString(73)}), // inner_signatures       | character varying(96)[]     |           |          |
				1,               // new_max_fee            | bigint                      |           |          |
				randomString(0), // account_muxed          | character varying(69)       |           |          |
				randomString(0), // fee_account_muxed      | character varying(69)       |           |          |
			}, nil

			// Check constraints:
			//     "valid_account_sequence" CHECK (account_sequence >= 0) NOT VALID
			//     "valid_application_order" CHECK (application_order >= 0) NOT VALID
			//     "valid_fee_charged" CHECK (fee_charged > 0) NOT VALID
			//     "valid_ledger_sequence" CHECK (ledger_sequence > 0) NOT VALID
			//     "valid_max_fee" CHECK (max_fee >= 0) NOT VALID
			//     "valid_new_max_fee" CHECK (new_max_fee > 0) NOT VALID
			//     "valid_operation_count" CHECK (operation_count >= 0) NOT VALID
		},
		Before: func(ctx context.Context, session db.SessionInterface) error {
			_, err := session.ExecRaw(ctx, `
				DROP INDEX IF EXISTS public.by_account;
				DROP INDEX IF EXISTS public.by_fee_account;
				DROP INDEX IF EXISTS public.by_hash;
				DROP INDEX IF EXISTS public.by_inner_hash;
				DROP INDEX IF EXISTS public.by_ledger;
				DROP INDEX IF EXISTS public.hs_transaction_by_id;
			`)
			return err
		},
		After: func(ctx context.Context, session db.SessionInterface) error {
			_, err := session.ExecRaw(ctx, `
				CREATE INDEX by_account on public.history_transactions (account, account_sequence)
				CREATE INDEX by_fee_account on public.history_transactions (fee_account) WHERE fee_account IS NOT NULL
				CREATE INDEX by_hash on public.history_transactions (transaction_hash)
				CREATE INDEX by_inner_hash on public.history_transactions (inner_transaction_hash) WHERE inner_transaction_hash IS NOT NULL
				CREATE INDEX by_ledger on public.history_transactions (ledger_sequence, application_order)
				-- TODO
				-- CREATE UNIQUE INDEX hs_transaction_by_id ON public.history_transactions (id)
			`)
			return err
		},
	},
	{
		Name: "history_operations",
		Columns: []string{
			"id",
			"transaction_id",
			"application_order",
			"type",
			// "details",
			"source_account",
			"source_account_muxed",
		},
		Generate: func(id uint64) ([]interface{}, error) {
			return []interface{}{
				id,
				0, // transaction_id       | bigint                |           | not null |
				0, // application_order    | integer               |           | not null |
				0, // type                 | integer               |           | not null |
				// TODO: Generate random json details
				// details              | jsonb                 |           |          |
				randomAddress(), // source_account       | character varying(64) |           | not null | ''::character varying
				"",              // source_account_muxed | character varying(69) |           |          |
			}, nil

			// Check constraints:
			//     "valid_application_order" CHECK (application_order >= 0) NOT VALID
		},
		Before: func(ctx context.Context, session db.SessionInterface) error {
			_, err := session.ExecRaw(ctx, `
				DROP INDEX IF EXISTS public.index_history_operations_on_id;
				DROP INDEX IF EXISTS public.index_history_operations_on_transaction_id;
				DROP INDEX IF EXISTS public.index_history_operations_on_type;
			`)
			return err
		},
		After: func(ctx context.Context, session db.SessionInterface) error {
			_, err := session.ExecRaw(ctx, `
				-- TODO
				-- CREATE UNIQUE INDEX index_history_operations_on_id ON public.history_operations (id);
				CREATE INDEX index_history_operations_on_transaction_id ON public.history_operations (transaction_id);
				CREATE INDEX index_history_operations_on_type ON public.history_operations (type);
			`)
			return err
		},
	},
	{
		Name: "history_effects",
		Columns: []string{
			"history_operation_id",
			"history_account_id",
			"order",
			"type",
			// "details",
			"address_muxed",
		},
		Generate: func(id uint64) ([]interface{}, error) {
			return []interface{}{
				id, // history_operation_id | bigint                |           | not null |
				0,  // history_account_id   | bigint                |           | not null |
				0,  // order                | integer               |           | not null |
				0,  // type                 | integer               |           | not null |
				// TODO: Generate random json details
				// details              | jsonb                 |           |          |
				"", // address_muxed        | character varying(69) |           |          |
			}, nil

			// Check constraints:
			//   "valid_order" CHECK ("order" >= 0) NOT VALID
		},
		Before: func(ctx context.Context, session db.SessionInterface) error {
			_, err := session.ExecRaw(ctx, `
				DROP INDEX IF EXISTS public.hist_e_by_order;
				DROP INDEX IF EXISTS public.hist_e_id;
				DROP INDEX IF EXISTS public.index_history_effects_on_type;
				DROP INDEX IF EXISTS public.trade_effects_by_order_book;
			`)
			return err
		},
		After: func(ctx context.Context, session db.SessionInterface) error {
			_, err := session.ExecRaw(ctx, `
				-- TODO
				-- CREATE UNIQUE INDEX "hist_e_by_order" ON public.history_effects (history_operation_id, "order");
				-- CREATE UNIQUE INDEX "hist_e_id" ON public.history_effects (history_account_id, history_operation_id, "order");
				CREATE INDEX "index_history_effects_on_type" ON public.history_effects (type);
				CREATE INDEX "trade_effects_by_order_book" ON public.history_effects ((details ->> 'sold_asset_type'::text), (details ->> 'sold_asset_code'::text), (details ->> 'sold_asset_issuer'::text), (details ->> 'bought_asset_type'::text), (details ->> 'bought_asset_code'::text), (details ->> 'bought_asset_issuer'::text)) WHERE type = 33;
			`)
			return err
		},
	},
}

type Account struct {
	id      uint64
	address string
}

type Table struct {
	Name string
	// First column must be the ID.
	Columns  []string
	Generate func(id uint64) ([]interface{}, error)
	Before   func(ctx context.Context, session db.SessionInterface) error
	After    func(ctx context.Context, session db.SessionInterface) error
}

func (t *Table) Jobs(dbUrl string, multiplier, rows, perTxn int) ([]Job, uint64, error) {
	session, err := db.Open("postgres", dbUrl)
	if err != nil {
		return nil, 0, err
	}
	defer session.Close()

	// Find the max existing ID
	var offset uint64
	err = session.DB.QueryRow(fmt.Sprintf("SELECT %q FROM %q ORDER BY %q LIMIT 1", t.Columns[0], t.Name, t.Columns[0])).Scan(&offset)
	if err != nil {
		return nil, 0, err
	}

	total := uint64(rows)
	if total == 0 {
		// Find the existing count
		var count uint64
		err = session.DB.QueryRow(fmt.Sprintf("SELECT count(*) FROM %q", t.Name)).Scan(&count)
		if err != nil {
			return nil, 0, err
		}
		total = count * uint64(multiplier-1)
	}

	if perTxn <= 0 {
		perTxn = int(total)
	}

	jobs := []Job{}
	for i := uint64(0); i < total; i += uint64(perTxn) {
		toInsert := total - i
		if toInsert > uint64(perTxn) {
			toInsert = uint64(perTxn)
		}
		start := i + offset + 1
		end := start + toInsert
		jobs = append(jobs, func(ctx context.Context) (uint64, error) {
			session, err := db.Open("postgres", dbUrl)
			if err != nil {
				return 0, err
			}
			defer session.Close()

			if _, err := session.ExecRaw(ctx, header); err != nil {
				return 0, err
			}

			if err := session.Begin(); err != nil {
				return 0, err
			}

			stmt, err := session.GetTx().Prepare(pq.CopyInSchema("public", t.Name, t.Columns...))
			if err != nil {
				return 0, err
			}

			// TODO: Make sure there's no duplicates.
			// Add the addresses
			for id := start; id < end; id++ {
				args, err := t.Generate(id)
				if err != nil {
					return 0, err
				}
				if _, err := stmt.ExecContext(ctx, args...); err != nil {
					return 0, err
				}
			}

			// Run the query against the db.
			if _, err := stmt.ExecContext(ctx); err != nil {
				return 0, err
			}
			if err := stmt.Close(); err != nil {
				return 0, err
			}

			// Commit it
			return toInsert, session.Commit()
		})
	}

	return jobs, total, nil
}

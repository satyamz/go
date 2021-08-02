package main

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"time"

	"github.com/lib/pq"
	"golang.org/x/sync/errgroup"

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
	jobs := flag.Int("jobs", 1, "How many parallel jobs to split it into")
	flag.Parse()

	if *dbUrl == "" {
		log.Fatal("--db-url is required")
	}

  if *rows == 0 && *multiplier == 0 {
    log.Fatal("Either --rows or --multiplier are required")
  } else if *rows > 0 && *multiplier > 0 {
    log.Fatal("Cannot set both --rows and --multiplier")
  }

	tables := []Table{
		{
			Name:    "history_accounts",
			Columns: []string{"id", "address"},
			Generate: func(id uint64) ([]interface{}, error) {
				return []interface{}{id, randomAddress()}, nil
			},
			Before: `
			-- ALTER TABLE public.history_trades DROP CONSTRAINT IF EXISTS "history_trades_base_account_id_fkey";
			-- ALTER TABLE public.history_trades DROP CONSTRAINT IF EXISTS "history_trades_counter_account_id_fkey";
			-- DROP INDEX IF EXISTS public.index_history_accounts_on_address;
			-- DROP INDEX IF EXISTS public.index_history_accounts_on_id;
			`,
			After: `
      -- TODO
			-- CREATE UNIQUE INDEX index_history_accounts_on_address ON public.history_accounts USING btree (address);
			-- CREATE UNIQUE INDEX index_history_accounts_on_id ON public.history_accounts USING btree (id);
			-- ALTER TABLE public.history_trades ADD CONSTRAINT "history_trades_base_account_id_fkey" FOREIGN KEY (base_account_id) REFERENCES history_accounts(id);
			-- ALTER TABLE public.history_trades ADD CONSTRAINT "history_trades_counter_account_id_fkey" FOREIGN KEY (counter_account_id) REFERENCES history_accounts(id);
			`,
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
			Before: `
				DROP INDEX IF EXISTS public.by_account;
				DROP INDEX IF EXISTS public.by_fee_account;
				DROP INDEX IF EXISTS public.by_hash;
				DROP INDEX IF EXISTS public.by_inner_hash;
				DROP INDEX IF EXISTS public.by_ledger;
				DROP INDEX IF EXISTS public.hs_transaction_by_id;
			`,
			After: `
				CREATE INDEX by_account on public.history_transactions (account, account_sequence)
				CREATE INDEX by_fee_account on public.history_transactions (fee_account) WHERE fee_account IS NOT NULL
				CREATE INDEX by_hash on public.history_transactions (transaction_hash)
				CREATE INDEX by_inner_hash on public.history_transactions (inner_transaction_hash) WHERE inner_transaction_hash IS NOT NULL
				CREATE INDEX by_ledger on public.history_transactions (ledger_sequence, application_order)
        -- TODO
				-- CREATE UNIQUE INDEX hs_transaction_by_id ON public.history_transactions (id)
			`,
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
			Before: `
				DROP INDEX IF EXISTS public.index_history_operations_on_id;
				DROP INDEX IF EXISTS public.index_history_operations_on_transaction_id;
				DROP INDEX IF EXISTS public.index_history_operations_on_type;
			`,
			After: `
        -- TODO
				-- CREATE UNIQUE INDEX index_history_operations_on_id ON public.history_operations (id);
				CREATE INDEX index_history_operations_on_transaction_id ON public.history_operations (transaction_id);
				CREATE INDEX index_history_operations_on_type ON public.history_operations (type);
			`,
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
			Before: `
				DROP INDEX IF EXISTS public.hist_e_by_order;
				DROP INDEX IF EXISTS public.hist_e_id;
				DROP INDEX IF EXISTS public.index_history_effects_on_type;
				DROP INDEX IF EXISTS public.trade_effects_by_order_book;
			`,
			After: `
        -- TODO
				-- CREATE UNIQUE INDEX "hist_e_by_order" ON public.history_effects (history_operation_id, "order");
				-- CREATE UNIQUE INDEX "hist_e_id" ON public.history_effects (history_account_id, history_operation_id, "order");
				CREATE INDEX "index_history_effects_on_type" ON public.history_effects (type);
				CREATE INDEX "trade_effects_by_order_book" ON public.history_effects ((details ->> 'sold_asset_type'::text), (details ->> 'sold_asset_code'::text), (details ->> 'sold_asset_issuer'::text), (details ->> 'bought_asset_type'::text), (details ->> 'bought_asset_code'::text), (details ->> 'bought_asset_issuer'::text)) WHERE type = 33;
			`,
		},
	}

	for _, t := range tables {
		if *table != "" && t.Name != *table {
			log.Println("Skipping table:", t.Name)
			continue
		}
		log.Println("Duplicating table:", t.Name)
		start := time.Now()
		nInserted, err := t.Duplicate(*dbUrl, *multiplier, *rows, *jobs)
		if err != nil {
			log.Fatal(err)
		}
		log.Println("Done:", nInserted, "records in", time.Since(start))
	}
}

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

type Account struct {
	id      uint64
	address string
}

type Table struct {
	Name string
	// First column must be the ID.
	Columns  []string
	Generate func(id uint64) ([]interface{}, error)
	Before   string
	After    string
}

func (t *Table) Duplicate(dbUrl string, multiplier, rows, jobs int) (uint64, error) {
	session, err := db.Open("postgres", dbUrl)
	if err != nil {
		return 0, err
	}
	defer session.Close()

	// Find the max existing ID
	var offset uint64
	err = session.DB.QueryRow("SELECT \"" + t.Columns[0] + "\" FROM \"" + t.Name + "\" ORDER BY id desc limit 1").Scan(&offset)
	if err != nil {
		return 0, err
	}
  log.Println("Max existing id for", t.Name, "is", offset)

	if t.Before != "" {
		if _, err := session.ExecRaw(context.Background(), t.Before); err != nil {
			return 0, err
		}
	}


  total := uint64(rows)
  if total == 0 {
    // Find the existing count
    var count uint64
    err = session.DB.QueryRow("SELECT count(*) FROM \"" + t.Name + "\"").Scan(&count)
    if err != nil {
      return 0, err
    }
    total = count * uint64(multiplier-1)
  }

	perJob := total / uint64(jobs)
  log.Println("Generating", total, "new rows for", t.Name, "in", jobs, "jobs")

	group, ctx := errgroup.WithContext(context.Background())
	for i := 0; i < jobs; i++ {
		job := i
		group.Go(func() error {
			session, err := db.Open("postgres", dbUrl)
			if err != nil {
				return err
			}
			defer session.Close()

			if _, err := session.ExecRaw(context.Background(), header); err != nil {
				return err
			}

			if err := session.Begin(); err != nil {
				return err
			}

			stmt, err := session.GetTx().Prepare(pq.CopyInSchema("public", t.Name, t.Columns...))
			if err != nil {
				return err
			}

			// TODO: Make sure there's no duplicates.
			// Add the addresses
			start := (uint64(job) * perJob) + offset + 1
			end := start + perJob
			for id := start; id < end; id++ {
				args, err := t.Generate(id)
				if err != nil {
					return err
				}
				if _, err := stmt.ExecContext(ctx, args...); err != nil {
					return err
				}
			}

			// Run the query against the db.
			if _, err := stmt.ExecContext(ctx); err != nil {
				return err
			}
			if err := stmt.Close(); err != nil {
				return err
			}

			// Commit it
			return session.Commit()
		})
	}

	if err := group.Wait(); err != nil {
		return 0, err
	}

	if t.After != "" {
		if _, err := session.ExecRaw(context.Background(), t.After); err != nil {
			return 0, err
		}
	}

	return total, nil
}

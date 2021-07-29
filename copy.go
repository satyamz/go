package main

import (
	"context"
	"flag"
	"log"
	"math/rand"

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

SET default_table_access_method = heap;
`

func main() {
	dbUrl := flag.String("db-url", "", "Database Url")
	multiplier := flag.Int("multiplier", 2, "How many times to multiply the database size")
	jobs := flag.Int("jobs", 1, "How many parallel jobs to split it into")
	flag.Parse()

	if *dbUrl == "" {
		log.Fatal("--db-url is required")
	}

	err := (&Table{
		Name:    "history_accounts",
		Columns: []string{"id", "address"},
		Generate: func(id uint64) ([]interface{}, error) {
			return []interface{}{id, randomAddress()}, nil
		},
		Before: `
			ALTER TABLE public.history_trades DROP CONSTRAINT IF EXISTS "history_trades_base_account_id_fkey";
			ALTER TABLE public.history_trades DROP CONSTRAINT IF EXISTS "history_trades_counter_account_id_fkey";
			DROP INDEX IF EXISTS public.index_history_accounts_on_address;
			DROP INDEX IF EXISTS public.index_history_accounts_on_id;
		`,
		After: `
			CREATE UNIQUE INDEX index_history_accounts_on_address ON public.history_accounts USING btree (address);
			CREATE UNIQUE INDEX index_history_accounts_on_id ON public.history_accounts USING btree (id);

			-- TODO 
			-- CREATE CONSTRAINT IF EXISTS "history_trades_base_account_id_fkey" FOREIGN KEY (base_account_id) REFERENCES history_accounts(id)
			-- CREATE CONSTRAINT IF EXISTS "history_trades_counter_account_id_fkey" FOREIGN KEY (counter_account_id) REFERENCES history_accounts(id)
		`,
	}).duplicate(*dbUrl, *multiplier, *jobs)
	if err != nil {
		log.Fatal(err)
	}
}

var base32Alphabet = []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ234567")

func randomAddress() string {
	n := 56
	b := make([]byte, n)
	for i := range b {
		b[i] = base32Alphabet[rand.Intn(len(base32Alphabet))]
	}
	return string(b)
}

type Table struct {
	Name string
	// First column must be the ID.
	Columns  []string
	Generate func(id uint64) ([]interface{}, error)
	Before   string
	After    string
}

func (t *Table) duplicate(dbUrl string, multiplier, jobs int) error {
	session, err := db.Open("postgres", dbUrl)
	if err != nil {
		return err
	}
	defer session.Close()

	// Find the existing count
	var count uint64
	err = session.DB.QueryRow("SELECT count(*) FROM " + t.Name).Scan(&count)
	if err != nil {
		return err
	}

	// Find the max existing ID
	var offset uint64
	err = session.DB.QueryRow("SELECT " + t.Columns[0] + " FROM " + t.Name + " ORDER BY id desc limit 1").Scan(&offset)
	if err != nil {
		return err
	}

	if _, err := session.ExecRaw(context.Background(), t.Before); err != nil {
		return err
	}

	total := count * uint64(multiplier-1)
	perJob := total / uint64(jobs)

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
		return err
	}

	if _, err := session.ExecRaw(context.Background(), t.After); err != nil {
		return err
	}

	return nil
}

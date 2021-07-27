package main

import (
	"context"
	"flag"
	"log"
	"math/rand"

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

SET default_table_access_method = heap;

-- Drop the indices while loading.
ALTER TABLE public.history_trades DROP CONSTRAINT IF EXISTS "history_trades_base_account_id_fkey";
ALTER TABLE public.history_trades DROP CONSTRAINT IF EXISTS "history_trades_counter_account_id_fkey";
DROP INDEX IF EXISTS public.index_history_accounts_on_address;
DROP INDEX IF EXISTS public.index_history_accounts_on_id;
`

const footer = `
CREATE UNIQUE INDEX index_history_accounts_on_address ON public.history_accounts USING btree (address);
CREATE UNIQUE INDEX index_history_accounts_on_id ON public.history_accounts USING btree (id);

-- TODO 
-- CREATE CONSTRAINT IF EXISTS "history_trades_base_account_id_fkey" FOREIGN KEY (base_account_id) REFERENCES history_accounts(id)
-- CREATE CONSTRAINT IF EXISTS "history_trades_counter_account_id_fkey" FOREIGN KEY (counter_account_id) REFERENCES history_accounts(id)
`

func main() {
	dbUrl := flag.String("db-url", "", "Database Url")
	multiplier := flag.Int("multiplier", 2, "How many times to multiply the database size")
	// jobs := 8 // TODO: Use this.
	flag.Parse()

	if *dbUrl == "" {
		log.Fatal("--db-url is required")
	}

	session, err := db.Open("postgres", *dbUrl)
	if err != nil {
		log.Fatal(err)
	}

	// Find the existing count
	var count uint64
	err = session.DB.QueryRow("SELECT count(*) FROM history_accounts").Scan(&count)
	if err != nil {
		log.Fatal(err)
	}

	// Find the max existing ID
	var offset uint64
	err = session.DB.QueryRow("SELECT id FROM history_accounts ORDER BY id desc limit 1").Scan(&offset)
	if err != nil {
		log.Fatal(err)
	}

	if _, err := session.ExecRaw(context.Background(), header); err != nil {
		log.Fatal(err)
	}

	if err := session.Begin(); err != nil {
		log.Fatal(err)
	}

	stmt, err := session.GetTx().Prepare(pq.CopyInSchema("public", "history_accounts", "id", "address"))
	if err != nil {
		log.Fatal(err)
	}

	inserted := map[string]struct{}{}

	// TODO: Make sure there's no duplicates.
	// Add the addresses
	for i := uint64(0); i < count*uint64(*multiplier-1); i++ {
		addr := randomAddress()
		if _, ok := inserted[addr]; ok {
			continue
		}
		inserted[addr] = struct{}{}
		if _, err := stmt.Exec(offset+i+1, addr); err != nil {
			log.Fatal(err)
		}
	}

	// Run the query against the db.
	if _, err := stmt.Exec(); err != nil {
		log.Fatal(err)
	}
	if err := stmt.Close(); err != nil {
		log.Fatal(err)
	}

	// Commit it
	if err := session.Commit(); err != nil {
		log.Fatal(err)
	}

	if _, err := session.ExecRaw(context.Background(), footer); err != nil {
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

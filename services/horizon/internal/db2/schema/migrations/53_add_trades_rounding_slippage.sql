-- +migrate Up

-- Add is_dust to trades table
ALTER TABLE history_trades ADD rounding_slippage numeric(1000, 7);

-- +migrate Down
ALTER TABLE history_trades DROP rounding_slippage;

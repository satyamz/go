-- +migrate Up

-- Add is_dust to trades table
ALTER TABLE history_trades ADD rounding_slippage bigint;
ALTER TABLE history_trades ADD base_reserves bigint;
ALTER TABLE history_trades ADD counter_reserves bigint;

-- +migrate Down
ALTER TABLE history_trades DROP rounding_slippage;
ALTER TABLE history_trades DROP base_reserves;
ALTER TABLE history_trades DROP counter_reserves;

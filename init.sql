CREATE DATABASE IF NOT EXISTS ordersdb;
USE ordersdb;

CREATE TABLE IF NOT EXISTS slot_rounds (
  round_id VARCHAR(64) PRIMARY KEY,
  player_id BIGINT NOT NULL,
  agent_id BIGINT NOT NULL,
  game_code VARCHAR(32) NOT NULL,
  bet_amount DECIMAL(18,2) NOT NULL,
  net_win DECIMAL(18,2) NOT NULL,
  currency VARCHAR(10) NOT NULL,
  balance_before DECIMAL(18,2) NOT NULL,
  balance_after DECIMAL(18,2) NOT NULL,
  is_free_spin BOOLEAN NOT NULL DEFAULT FALSE,
  jackpot_win DECIMAL(18,2) NOT NULL DEFAULT 0,
  round_time DATETIME NOT NULL
);

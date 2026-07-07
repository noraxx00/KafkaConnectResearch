-- Raw stream over the existing slot_rounds topic.
-- The producer writes a Kafka Connect JSON envelope ({"schema":..., "payload": {...}})
-- so only the `payload` struct is declared here; the `schema` field is simply
-- never referenced and ksqlDB ignores it.
CREATE STREAM slot_rounds_raw (
  payload STRUCT<
    round_id VARCHAR,
    player_id BIGINT,
    agent_id BIGINT,
    game_code VARCHAR,
    bet_amount DOUBLE,
    net_win DOUBLE,
    currency VARCHAR,
    balance_before DOUBLE,
    balance_after DOUBLE,
    is_free_spin BOOLEAN,
    jackpot_win DOUBLE,
    round_time VARCHAR
  >
) WITH (
  KAFKA_TOPIC = 'slot_rounds',
  VALUE_FORMAT = 'JSON'
);

-- Re-key each round by "player_id_game_code" and compute its net_win.
-- PARTITION BY forces a repartition (new topic slot_rounds_keyed, hidden
-- inside this derived stream) so the Kafka message key becomes the composite
-- key we actually want to aggregate on -- the source topic is keyed by
-- round_id, which is useless for a per-player+game running sum.
CREATE STREAM slot_rounds_keyed
  WITH (KAFKA_TOPIC = 'slot_rounds_keyed', VALUE_FORMAT = 'JSON') AS
  SELECT
    CONCAT(CAST(payload->player_id AS VARCHAR), '_', payload->game_code) AS player_game_key,
    payload->net_win - payload->bet_amount AS net_win
  FROM slot_rounds_raw
  PARTITION BY CONCAT(CAST(payload->player_id AS VARCHAR), '_', payload->game_code)
  EMIT CHANGES;

-- Running sum of net_win per player+game, materialized as a compacted table
-- topic (player_game_netwin) -- this IS the Redis-ready dataset.
-- WRAP_SINGLE_VALUE=false makes the Kafka message value the bare number
-- (e.g. 42.5) instead of {"NET_WIN":42.5}, so the Redis sink writes a plain
-- scalar rather than a JSON object.
CREATE TABLE player_game_netwin
  WITH (KAFKA_TOPIC = 'player_game_netwin', VALUE_FORMAT = 'JSON', WRAP_SINGLE_VALUE = false) AS
  SELECT
    player_game_key,
    SUM(net_win) AS net_win
  FROM slot_rounds_keyed
  GROUP BY player_game_key
  EMIT CHANGES;

# Runbook — Local Setup

All commands below assume a working directory of `kafka-test/` unless stated otherwise. Written for PowerShell (`curl.exe`, not the `curl` alias).

## 1. Start Kafka and Connect
```powershell
docker compose up -d --force-recreate kafka
docker compose logs -f kafka   # wait for it to fully boot before continuing

docker compose up -d kafka-connect
curl -s localhost:8083/connectors   # wait until it responds
```

## 2. Create the slot_rounds topic
```powershell
docker compose exec kafka kafka-topics --create --topic slot_rounds --partitions 6 --replication-factor 1 --bootstrap-server localhost:9092

# inspect current offsets
docker compose exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic slot_rounds
```

## 3. MariaDB sink
```powershell
curl.exe -X POST -H "Content-Type: application/json" -d "@connectors/mariadb-slot-rounds-sink.json" http://localhost:8083/connectors
curl.exe -s http://localhost:8083/connectors/mariadb-slot-rounds-sink/status

docker compose exec mariadb mariadb -uorders -porders ordersdb -e "DESCRIBE slot_rounds;"
docker compose exec mariadb mariadb -uorders -porders ordersdb -e "SELECT COUNT(*) FROM slot_rounds;"
```

If the table already exists from a previous run and `init.sql` was updated since (e.g. a new column), `init.sql` won't re-run automatically — alter the live table instead:
```powershell
docker compose exec mariadb mariadb -uorders -porders ordersdb -e "ALTER TABLE slot_rounds ADD COLUMN <col> <type> NOT NULL DEFAULT <default> AFTER <existing_col>;"
```

## 4. Redis sink (risk control)
The Redis connector plugin is pinned to **v0.9.1** — 1.0+ ships Java 17 bytecode, which silently fails to load under this image's Java 11 runtime (the plugin registers on the plugin path, but the class scanner finds 0 classes). Revisit this pin if the `kafka-connect` image is ever bumped to a Java 17 base.

```powershell
docker compose up -d --force-recreate kafka-connect   # re-runs the plugin install steps in the compose command
docker compose logs -f kafka-connect

# confirm the plugin actually loaded
curl.exe -s http://localhost:8083/connector-plugins   # should list com.redis.kafka.connect.RedisSinkConnector

curl.exe -X POST -H "Content-Type: application/json" -d "@connectors/redis-risk-control-sink.json" http://localhost:8083/connectors
curl.exe -s http://localhost:8083/connectors/redis-risk-control-sink/status

# inspect a hash — only a whitelisted subset of fields is written
# (player_id, game_code, bet_amount, net_win, jackpot_win, is_free_spin, round_time
#  — see transforms.selectRiskFields.include in the connector config)
docker compose exec redis redis-cli --scan --pattern "risk:round:*"
docker compose exec redis redis-cli HGETALL "risk:round:<round_id from above>"
```

**Known gap**: v0.9.1 has no `redis.key.ttl` (added in 1.1.0) — every round's hash is kept forever. At target load (20k/sec) that's unbounded Redis growth. Before the stress test, pick one:
- set `maxmemory-policy allkeys-lru` on the Redis instance, or
- run a housekeeping job to expire old `risk:round:*` keys, or
- bump to a Java 17 `kafka-connect` image to get native TTL support.

## 5. Produce test data
```powershell
cd producer
go run main.go -brokers=localhost:9092 -topic=slot_rounds -rate=5000 -duration=15s -workers=8
cd ..
```

## Resetting between runs
```powershell
curl.exe -X DELETE http://localhost:8083/connectors/mariadb-slot-rounds-sink
curl.exe -X DELETE http://localhost:8083/connectors/redis-risk-control-sink

docker compose exec kafka kafka-topics --delete --topic slot_rounds --bootstrap-server localhost:9092
docker compose exec kafka kafka-topics --create --topic slot_rounds --partitions 6 --replication-factor 1 --bootstrap-server localhost:9092
```
Then re-register the connectors and re-produce as in steps 3-5.

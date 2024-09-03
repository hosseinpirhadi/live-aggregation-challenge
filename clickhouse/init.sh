#!/bin/bash

# Start ClickHouse server
/opt/bitnami/scripts/clickhouse/run.sh &

# Wait for ClickHouse to be ready
sleep 5

# Execute SQL commands
clickhouse-client --host=localhost --user=default --multiquery --query="
CREATE DATABASE IF NOT EXISTS financial;

USE financial;

CREATE TABLE kafka_data (
    payload Tuple(
        before Tuple(
            price Nullable(Int32),
            volume Nullable(Int32),
            datetime_created Int64
        ),
        after Tuple(
            price Nullable(Int32),
            volume Nullable(Int32),
            datetime_created Int64
        )
    ),
    source Tuple(
        version String,
        connector String,
        name String,
        ts_ms Int64,
        snapshot Nullable(String),
        db String,
        sequence Nullable(String),
        ts_us Nullable(Int64),
        ts_ns Nullable(Int64),
        schema String,
        table String,
        txId Nullable(Int64),
        lsn Nullable(Int64),
        xmin Nullable(Int64)
    ),
    transaction Tuple(
        id Nullable(String),
        total_order Nullable(Int64),
        data_collection_order Nullable(Int64)
    ),
    op String,
    ts_ms Nullable(Int64),
    ts_us Nullable(Int64),
    ts_ns Nullable(Int64)
)
ENGINE = Kafka(
  'kafka:9092',
  'dbserver1.bank.holding',
  'consumer-group-holding',
  'JSONEachRow'
);

SET stream_like_engine_allow_direct_select=1;
SET allow_experimental_live_view=1;

CREATE TABLE raw_data (
    price Nullable(Int32),
    volume Nullable(Int32),
    datetime_created DateTime('Asia/Tehran')
) ENGINE = MergeTree()
ORDER BY datetime_created;

CREATE MATERIALIZED VIEW kafka_data_to_raw_data
TO raw_data
AS
SELECT
    payload.after.price AS price,
    payload.after.volume AS volume,
    (payload.after.datetime_created / 1000000) AS datetime_created
FROM kafka_data
WHERE payload.after IS NOT NULL;

CREATE LIVE VIEW min_max_price_last_hour AS
SELECT
    min(price) AS min_price,
    max(price) AS max_price,
    toStartOfHour(datetime_created) AS hour
FROM raw_data
WHERE datetime_created >= now('Asia/Tehran') - INTERVAL 1 HOUR
GROUP BY hour;

CREATE LIVE VIEW open_price_last_hour AS
SELECT
    price AS open_price,
    toStartOfHour(datetime_created) AS hour,
    datetime_created
FROM raw_data
WHERE datetime_created >= now() - INTERVAL 1 HOUR
ORDER BY datetime_created ASC
LIMIT 1 BY hour;

CREATE LIVE VIEW close_price_last_hour AS
SELECT
    price AS close_price,
    toStartOfHour(datetime_created) AS hour,
    datetime_created
FROM raw_data
WHERE datetime_created >= now() - INTERVAL 1 HOUR
ORDER BY datetime_created DESC
LIMIT 1 BY hour;

CREATE VIEW max_drawdown_view_2 AS
SELECT 
    max(drawdown) AS maxdrawdown
FROM (
    SELECT
        high.datetime_created AS highdate,
        low.datetime_created AS lowdate,
        (high.price - low.price) AS drawdown
    FROM (
        SELECT
            datetime_created,
            price
        FROM raw_data
        ORDER BY datetime_created
    ) AS high
    CROSS JOIN (
        SELECT
            datetime_created,
            price
        FROM raw_data
        ORDER BY datetime_created
    ) AS low
    WHERE high.datetime_created < low.datetime_created
) AS subquery;
"

# Keep the container running
wait

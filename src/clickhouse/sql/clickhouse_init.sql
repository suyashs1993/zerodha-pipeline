CREATE DATABASE IF NOT EXISTS zerodhadata;
USE zerodhadata;

CREATE TABLE IF NOT EXISTS ticks_raw (
            ts DateTime64(6, 'UTC'),
            instrument_token UInt64,
            avg_traded_price Float64,
            last_price Float64,
            volume UInt64,
            buy_qty UInt64,
            sell_qty UInt64
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMMDD(ts)
        ORDER BY (instrument_token, ts)
        TTL toDateTime(ts) + INTERVAL 90 DAY
        SETTINGS index_granularity = 8192;

SET allow_experimental_object_type = 1;

CREATE TABLE IF NOT EXISTS orderbook_snapshot (
            ts DateTime64(6, 'UTC'),
            instrument_token UInt64,
            best_bid  Float64,
            best_ask  Float64,
            imbalance_ratio Float64,
            bid_ask_spread Float64,
            mid_price Float64,
            weighted_mid_price Float64,
            order_count_imbalance Float64,
            depth_json JSON CODEC(ZSTD(3))
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMMDD(ts)
        ORDER BY (instrument_token, ts)
        TTL toDateTime(ts) + INTERVAL 7 DAY
        SETTINGS index_granularity = 8192;


 CREATE TABLE IF NOT EXISTS candles_1m (
            bucket DateTime('UTC'),
            instrument_token UInt64,
            open Float64,
            high Float64,
            low Float64,
            close Float64,
            volume UInt64
        )
        ENGINE = ReplacingMergeTree()
        PARTITION BY toYYYYMMDD(bucket)
        ORDER BY (instrument_token, bucket);

 CREATE MATERIALIZED VIEW IF NOT EXISTS mv_candles_1m
        TO candles_1m
        AS
        SELECT
            toStartOfMinute(ts) AS bucket,
            instrument_token,
            argMin(last_price, ts) AS open,
            max(last_price) AS high,
            min(last_price) AS low,
            argMax(last_price, ts) AS close,
            sum(volume) AS volume
        FROM ticks_raw
        GROUP BY instrument_token, bucket;

 CREATE TABLE IF NOT EXISTS candles_5m (
            bucket DateTime('UTC'),
            instrument_token UInt64,
            open Float64,
            high Float64,
            low Float64,
            close Float64,
            volume UInt64
        )
        ENGINE = ReplacingMergeTree()
        PARTITION BY toYYYYMMDD(bucket)
        ORDER BY (instrument_token, bucket);



 CREATE MATERIALIZED VIEW IF NOT EXISTS mv_candles_5m
        TO candles_5m
        AS
        SELECT
            toStartOfInterval(bucket, INTERVAL 5 MINUTE) AS bucket_5m,
            instrument_token,
            argMin(open, bucket) AS open,
            max(high) AS high,
            min(low) AS low,
            argMax(close, bucket) AS close,
            sum(volume) AS volume
        FROM candles_1m
        GROUP BY instrument_token, bucket_5m;


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
        TTL toDateTime(ts) + INTERVAL 30 DAY
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
        TTL toDateTime(ts) + INTERVAL 30 DAY
        SETTINGS index_granularity = 8192;


 CREATE TABLE IF NOT EXISTS candles_1m (
            bucket DateTime64(6, 'UTC'),
            instrument_token UInt64,
            open AggregateFunction(argMin, Float64, DateTime64(6, 'UTC')),
            high AggregateFunction(max, Float64),
            low AggregateFunction(min, Float64),
            close AggregateFunction(argMax, Float64, DateTime64(6, 'UTC')),
            volume AggregateFunction(sum, UInt64),
        )
        ENGINE = AggregatingMergeTree()
        PARTITION BY toYYYYMMDD(bucket)
        ORDER BY (instrument_token, bucket);

 CREATE MATERIALIZED VIEW IF NOT EXISTS mv_candles_1m
        TO candles_1m
        AS
        SELECT
            toStartOfMinute(ts) AS bucket,
            instrument_token,
            argMinState(last_price, ts) AS open,
            maxState(last_price) AS high,
            minState(last_price) AS low,
            argMaxState(last_price, ts) AS close,
            sumState(volume) AS volume
        FROM ticks_raw
        GROUP BY instrument_token, bucket;



 CREATE TABLE IF NOT EXISTS candles_5m (
            bucket_5m DateTime64(6, 'UTC'),
            instrument_token UInt64,
            open AggregateFunction(argMin, Float64, DateTime64(6, 'UTC')),
            high AggregateFunction(max, Float64),
            low AggregateFunction(min, Float64),
            close AggregateFunction(argMax, Float64, DateTime64(6, 'UTC')),
            volume AggregateFunction(sum, UInt64),
        )
        ENGINE = AggregatingMergeTree()
        PARTITION BY toYYYYMMDD(bucket_5m)
        ORDER BY (instrument_token, bucket_5m);



 CREATE MATERIALIZED VIEW IF NOT EXISTS mv_candles_5m
        TO candles_5m
        AS
        SELECT
            toStartOfInterval(ts, INTERVAL 5 MINUTE) AS bucket_5m,
            instrument_token,
            argMinState(last_price, ts) AS open,
            maxState(last_price) AS high,
            minState(last_price) AS low,
            argMaxState(last_price, ts) AS close,
            sumState(volume) AS volume
        FROM ticks_raw
        GROUP BY instrument_token, bucket_5m;


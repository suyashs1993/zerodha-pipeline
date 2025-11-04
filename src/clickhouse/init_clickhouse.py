import os
import clickhouse_connect


def init_clickhouse():
    # --- Connection configuration ---
    #host='localhost'
    host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    port = int(os.getenv("CLICKHOUSE_PORT", 8123))  # HTTP port
    user = os.getenv("CLICKHOUSE_USER", "default")
    password = os.getenv("CLICKHOUSE_PASSWORD", "")
    db_name = os.getenv("CLICKHOUSE_DB", "zerodhadata")

    # --- Connect to ClickHouse (default DB) ---
    client = clickhouse_connect.get_client(
        host=host,
        port=port,
        username=user,
        password=password
    )
    print("✅ Connected to ClickHouse via HTTP")


    # --- Create database if not exists ---
    client.command(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    print(f"📦 Database '{db_name}' created or already exists")

    # --- Reconnect to use that database ---
    client = clickhouse_connect.get_client(
        host=host,
        port=port,
        username=user,
        password=password,
        database=db_name
    )
    client.command("""SET 
    allow_experimental_object_type = 1""")

    # --- Create ticks_raw table ---
    client.command("""
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
        SETTINGS index_granularity = 8192
    """)
    print("📊 Created table: ticks_raw")

    # --- Create orderbook_snapshot table ---
    client.command("""
        CREATE TABLE IF NOT EXISTS orderbook_snapshot (
            ts DateTime64(6, 'UTC'),
            instrument_token UInt64,
            imbalance Float64,
            spread Float64,
            mid_price Float64,
            weighted_mid_price Float64,
            order_count_imbalance Float64,
            depth JSON CODEC(ZSTD(6))
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMMDD(ts)
        ORDER BY (instrument_token, ts)
        TTL toDateTime(ts) + INTERVAL 7 DAY
    """)
    print("📊 Created table: orderbook_snapshot")

    client.command("""
          CREATE TABLE IF NOT EXISTS kafka_snapshots
(
            ts DateTime64(6, 'UTC'),
            instrument_token UInt64,
            imbalance Float64,
            spread Float64,
            mid_price Float64,
            weighted_mid_price Float64,
            order_count_imbalance Float64,
            depth JSON CODEC(ZSTD(6))
            ts_emit DateTime64(6, 'UTC')
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:9092',
         kafka_topic_list = 'zerodhadata.snapshots.1s',
         kafka_group_name = 'ch_snapshots_group',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1""")
    print("📊 Created table: kafka_snapshot")

    client.command("""
              CREATE MATERIALIZED VIEW IF NOT EXISTS mv_snapshots TO market.market_snapshots AS
        SELECT
        toDateTime64(timestamp/1000, 3) AS timestamp,
        instrument_token UInt64,
        imbalance Float64,
        spread Float64,
        mid_price Float64,
        weighted_mid_price Float64,
        order_count_imbalance Float64,
        depth JSON CODEC(ZSTD(6))
        FROM kafka_snapshots""")

    print("📊 Created table: mv_snapshots")


    # --- Create audit_trail table ---
    client.command("""
        CREATE TABLE IF NOT EXISTS audit_trail (
            ts DateTime64(6, 'UTC'),
            instrument_token UInt64,
            delta_depth JSON CODEC(ZSTD(6))
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMMDD(ts)
        ORDER BY (instrument_token, ts)
        TTL toDateTime(ts) + INTERVAL 7 DAY
    """)
    print("📊 Created table: audit_trail")

    print("✅ ClickHouse schema initialized successfully.")

    # client.command("""
    #     CREATE FUNCTION IF NOT EXISTS toStartOfFiveMinute AS (x) -> toStartOfInterval(x, INTERVAL 5 MINUTE);
    # """)

    client.command("""
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
    """)
    print("✅ ClickHouse candle_1m created  successfully.")
    client.command("""
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
    """)
    print("✅ ClickHouse candle_mv_1m created  successfully.")

    client.command("""
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
    """)
    print("✅ ClickHouse candle_5m created  successfully.")
    client.command("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv_candles_5m
        TO candles_5m
        AS
        SELECT
            toStartOfFiveMinute(bucket) AS bucket_5m,
            instrument_token,
            argMin(open, bucket) AS open,
            max(high) AS high,
            min(low) AS low,
            argMax(close, bucket) AS close,
            sum(volume) AS volume
        FROM candles_1m
        GROUP BY instrument_token, bucket_5m;
    """)

print("✅ ClickHouse candle_5m_mv created  successfully.")
if __name__ == "__main__":
    init_clickhouse()

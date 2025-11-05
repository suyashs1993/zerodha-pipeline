Project Overview

# Zerodha Real-Time Data Pipeline

## Overview
This application ingests real-time tick data from Zerodha  websocket API, processes order book metrics from the market depth,
and generates signals. The focus is on handling high throughput with low processing latency. 

## Installation and Setup

1. Clone this repository to your System
2. Install docker in your system
3. Generate access token using your Api key for Kite Connect.
4. Tech stack used in this project includes Kite-Connect SDK for consuming zerodha websocket API, **Redpanda**, **Clickhouse** and version of Python 3.8 or higher. 
All of this can be set up by running the docker with command  "docker-compose up -d --build"
                
5. Running it brings all the services up . Check if a database is created with name **zerodhadata** in Clickhouse DB.

6. Clickhouse DB runs at localhost:8123. You can query it in following way "http://localhost:8123/?query=SHOW+DATABASES"
    This database and all the tables in it are created with a sql script with path src/clickhouse/sql/clickhouse_init.sql
    which runs at startup
7.  As the services are running we can view the log for ingest service that processes the incoming ticks by command
"docker-compose logs -f ingest "


## System Design
 
Functional Requirements

1. Should be able to consume incoming tick data from Zerodha websocket API for atleast 50 stocks from NIFTY500.
2. Calculate orderbook metrics from the market depth present in raw tick data.
3. Generate a snapshot of orderbook every second containing metrics and full market depth
4. Based on order imbalance ratio, generate BUY, HOLD or SELL signals
5. Give functions to query data for a instrument and given time range. Also provide 
   capabilities to generate 1m, 5m candle (OHLCV) windows

Non Functional Requirements
1. Write Throughput > 10,000 ticks/sec. 
2. Query Latency
3. Processing Latency < 50 ms
4. Storage Efficiency <50% of raw


# High Level Design


                   │     Zerodha Market API      │
                   │    (Tick-by-tick Stream)    │
                   └───────────────┬─────────────┘
                                   │ Live ticks
                                   ▼
                      ┌────────────────────────┐
                      │     Ingestion Service   │
                      │  (WebSocket Receiver)   │
                      ├────────────────────────┤
                      │ In-Memory Queue (max    │
                      │ size: 20,000)           │
                      │                         │
                      │ When queue ≥ BATCH_SIZE │
                      │ → **Flush batch to      │
                      │    ClickHouse first**   │
                      │    (sub-50ms latency)   │
                      │                         │
                      │ After flush → Publish   │
                      │ ticks to Redpanda       │
                      └──────────┬──────────────┘
                                 │
                         Publishes ticks
                                 │
                                 ▼
                   ┌─────────────────────────────┐
                   │       Redpanda / Kafka       │
                   │       Topic: ticks_raw       │
                   └───────────┬─────────┬───────┘
                               │         │
                     Consumer A│         │Consumer B
                               │         │
                               ▼         ▼
      ┌──────────────────────────────┐   ┌────────────────────────┐
      │     Orderbook_Snapshot       │   │     Signal Service     │
      │     (Snapshot Generator)     │   │   (Strategy Logger)    │
      ├──────────────────────────────┤   ├────────────────────────┤
      │ - Maintains orderbook state  │   │ - Reads same ticks     │
      │ - Compute metrics every sec  │   │ - Applies logic        │
      │ - Batch insert snapshots     │   │ - Writes signals to    │
      │                  → CH        │   │   log file             │
      └─────── ───────┬──────────────┘   └─────────────┬──────────┘
                      │                                │
                      ▼                                ▼
            ┌────────────────────┐           ┌─────────────────────┐
            │   ClickHouse DB    │           │   Signal Log Files  │
            │ (Snapshot Storage) │           │ (Local / Mounted)   │
            └────────────────────┘           └─────────────────────┘



Choice of DB

Clickhouse was chosen for its fast writes for high batch size writes and low latency
querying capabilities for aggregated data like candle windows. It is well designed to handle high throughput time series
data. The other choice isTimescaleDb but ACID guarantee and relational joins are not the main priority of out system
here we ca

Choice of Queue
Queue provides some durability and it also gives buffer to calculate orderbook metrics. By having a queue we can independently scale
Orderbook_Snapshot service in case it lags behind . The downside is added latency, but  for orderbook data we 
are only storing snapshot every second and not real time. Redpanda is easier to setup with less infra 
and it is faster than Kafka, so it was used here. 


## Services

Ingest Service

1. Pulls NSE instrument tokens for 3000 instruments (max limit for 1 web socket connection)
2. Subscribe to these instruments and start processing incoming ticks (Throughput ~ 1000 ticks/second)
3.  Flush to clickhouse DB when queue_size > batch size (latency < 50)
4.  Publishes the batched data in batch to Redpanda Topic.


Optimizations

1. Clickhouse DB insertion is done here using async/await (asyncio library) ,so that while insertion 
incoming tick processing is not blocked . However only after insertion happens, the flow moves to publishing to Redpanda
This is done because, writes are quiet fast in Clickhouse so there is very little lag

2. While publishing to topic, we are sending the message asynchronously and we are also batching them by specifying the
batch size while initialising the producer. So this means less to and fro network calls for sending the whole data.

Areas of Improvement
1. Not fault tolerant. We are maintaining an in-memory queue initially.

Orderbook Snapshot Service

1. Consumes from the topic and maintains a in-memory buffer- a dictionary containing the order book data for a given instrument(key)
2. Calculates the order book metrics while processing the tick data in a loop and after a second
elapses  flushes it along with  he full market depth json in compressed format in clickhouse DB.

Areas of Improvement
1. More worker services  to consume the data and partitioning the topic can scale the system better. With the current design there might be lag 
as we are processing the metrics and flushing to DB in a synchronous way.
2. Tools like Apache Flink can be used for scheduled writes per second.


Signal Service
1. Consumes the data from same topic and calculates the signal data and writes it into a log file.

Areas of Improvement
1. The setup for streaming the signals is there but graphana dashboard or frontend will be suitable for visualization.

Query Utils
1. Function for fetching tick data for an instrument within a given time range.
2. Function for generating candle (OHLCV) 1m , 5m windows.
3. Function for retrieving order book data for an instrument within given time range


## Tables ##

ticks_raw  - Stores the tick data (excluding depth)

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


orderbook_snapshot  - Stores the snap shot of orderbook

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


Both tables have partition on day. While the primary key is instrument token and timestamp. This enables faster query times
when both variables are given in query.

The tables for OHLCV for 1 and 5m  also have materialized views for them so that aggregations are faster. The script for
generating tables is clickhouse_init.sql as mentioned before. It runs at startup time if you start through Docker


## BenchMarks ##

1. Incoming tick data - (Processing latency < 50 ms, Compression > 50% as LZ4 compression 
in Clickhouse is enabled by default)
2. Orderbook data - Processing latency ~ 200-400 ms. Compression  > 50 % as the biggest column by size, the
  market depth is compressed using ZSTD before persisting to Clickhouse
3. Query Latency < 100 ms as tables are optimized for reads for parameters in the query i.e. timestamp and instrument
token. Candle window tables are also fast as they are using materialized views



   


























































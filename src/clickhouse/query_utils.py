"""
query_utils.py
-------------------
Helper functions for querying tick data and generating OHLCV candles from ClickHouse.

Features:
1. Time-range tick queries from ClickHouse
2. Aggregated candle generation (1min, 5min, etc.)
3. Query by symbol or instrument_token
4. Efficient ClickHouse-based queries
"""

import pandas as pd
import datetime
import src.clickhouse.clickhouse_client as clickhouse_client



# =============================
# Helper: Get ClickHouse connection
# =============================
def get_client():
    """Get ClickHouse client connection."""
    return clickhouse_client.get_clickhouse_client()


# =============================
# 1️⃣ Helper: Fetch tick data for a time range
# =============================
def get_ticks(instrument_token: int,
              start_time: datetime.datetime,
              end_time: datetime.datetime,
              limit: int = None) -> pd.DataFrame:
    """
    Fetch raw tick data for a given instrument within a time range from ClickHouse.

    """
    client = get_client()
    start = start_time.strftime("%Y-%m-%d %H:%M:%S.%f")
    end = end_time.strftime("%Y-%m-%d %H:%M:%S.%f")
    
    # Build query
    query = f"""
        SELECT ts, instrument_token, avg_traded_price, last_price, volume, buy_qty, sell_qty
        FROM ticks_raw
        WHERE instrument_token = {instrument_token}
        AND  ts BETWEEN toDateTime64('{start}', 6, 'UTC')
        AND toDateTime64('{end}', 6, 'UTC')
        ORDER BY ts
    """
    
    if limit:
        query += f" LIMIT {limit}"
    
    # Execute query and return as DataFrame
    result = client.query(query)
    df = pd.DataFrame(result.result_rows, columns=result.column_names)
    
    if not df.empty:
        df['ts'] = pd.to_datetime(df['ts'])
        df = df.sort_values('ts').reset_index(drop=True)
    
    return df




# =============================
# 2️⃣ Helper: Aggregate to OHLCV candles (using ClickHouse views)
# =============================
def get_candles_1m(instrument_token: int,
                start_time: datetime.datetime,
                end_time: datetime.datetime,
                interval: str = "1min") -> pd.DataFrame:
    """
    Aggregate tick data into 1 min OHLCV candles from ClickHouse materialized views.

    Args:
        instrument_token: The instrument token to query
        start_time: Start of time range (datetime UTC)
        end_time: End of time range (datetime UTC)
        interval: Candle interval - '1min'
    Returns:
        DataFrame with columns: bucket, instrument_token, open, high, low, close, volume
    """

    client = get_client()

    start = start_time.strftime("%Y-%m-%d %H:%M:%S")
    end = end_time.strftime("%Y-%m-%d %H:%M:%S")

    query = f"""
        SELECT bucket, instrument_token, open, high, low, close, volume
        FROM candles_1m
        WHERE instrument_token = {instrument_token}
         AND  bucket BETWEEN toDateTime64('{start}', 6, 'UTC')
        AND toDateTime64('{end}', 6, 'UTC')
        ORDER BY bucket ASC
    """
    
    result = client.query(query)
    df = pd.DataFrame(result.result_rows, columns=result.column_names)
    
    if not df.empty:
        df['bucket'] = pd.to_datetime(df['bucket'])
        df = df.sort_values('bucket').reset_index(drop=True)
    
    return df


def get_candles_5m(instrument_token: int,
                   start_time: datetime.datetime,
                   end_time: datetime.datetime,
                   interval: str = "1min") -> pd.DataFrame:
    """
    Aggregate tick data into 5 min OHLCV candles from ClickHouse materialized views.

    Args:
        instrument_token: The instrument token to query
        start_time: Start of time range (datetime UTC)
        end_time: End of time range (datetime UTC)
        interval: Candle interval - '5min'
    Returns:
        DataFrame with columns: bucket, instrument_token, open, high, low, close, volume
    """

    client = get_client()

    start = start_time.strftime("%Y-%m-%d %H:%M:%S")
    end = end_time.strftime("%Y-%m-%d %H:%M:%S")

    query = f"""
        SELECT bucket_5m, instrument_token, open, high, low, close, volume
        FROM candles_5m
        WHERE instrument_token = {instrument_token}
         AND  bucket_5m BETWEEN toDateTime64('{start}', 6, 'UTC')
        AND toDateTime64('{end}', 6, 'UTC')
        ORDER BY bucket_5m ASC
    """

    result = client.query(query)
    df = pd.DataFrame(result.result_rows, columns=result.column_names)

    if not df.empty:
        df['bucket_5m'] = pd.to_datetime(df['bucket_5m'])
        df = df.sort_values('bucket_5m').reset_index(drop=True)

    return df

# =============================
# 3️⃣ Helper: Get latest tick for instrument
# =============================
def get_latest_tick(instrument_token: int) -> pd.Series:
    """
    Get the most recent tick for an instrument.
    
    Args:
        instrument_token: The instrument token to query
    
    Returns:
        Series with latest tick data
    """
    client = get_client()
    query = f"""
        SELECT ts, instrument_token, avg_traded_price, last_price, volume, buy_qty, sell_qty
        FROM ticks_raw
        WHERE instrument_token = {instrument_token}
        ORDER BY ts DESC
        LIMIT 1
    """
    
    result = client.query(query)
    
    if not result.result_rows:
        return pd.Series()
    
    df = pd.DataFrame(result.result_rows, columns=result.column_names)
    return df.iloc[0]


# =============================
# 4️⃣ Helper: Get order book metrics
# =============================
def get_orderbook_metrics(instrument_token: int,
                         start_time: datetime.datetime,
                         end_time: datetime.datetime) -> pd.DataFrame:
    """
    Get order book microstructure metrics for a time range.
    
    Args:
        instrument_token: The instrument token to query
        start_time: Start of time range (datetime UTC)
        end_time: End of time range (datetime UTC)
    
    Returns:
        DataFrame with columns: ts, instrument_token, imbalance, spread, mid_price, 
                                weighted_mid_price, order_count_imbalance
    """
    client = get_client()
    start = start_time.strftime("%Y-%m-%d %H:%M:%S.%f")
    end = end_time.strftime("%Y-%m-%d %H:%M:%S.%f")
    query = f"""
        SELECT ts, instrument_token, imbalance_ratio, bid_ask_spread, mid_price, weighted_mid_price, order_count_imbalance,depth_json
        FROM orderbook_snapshot
        WHERE instrument_token = {instrument_token}
        AND  ts BETWEEN toDateTime64('{start}', 6, 'UTC')
        AND toDateTime64('{end}', 6, 'UTC')
        ORDER BY ts ASC
    """
    
    result = client.query(query)
    df = pd.DataFrame(result.result_rows, columns=result.column_names)
    
    if not df.empty:
        df['ts'] = pd.to_datetime(df['ts'])
        df = df.sort_values('ts').reset_index(drop=True)
    
    return df



# =============================
# Example usage
# =============================
if __name__ == "__main__":
    # Example: Query ticks for RELIANCE in the last hour
    import datetime
    
    now = datetime.datetime.now(datetime.UTC)
    one_hour_ago = now - datetime.timedelta(days=2)

    # You'll need to replace with actual instrument token
    instrument_token = 	5889
    
    print("Fetching tick data...")
    ticks = get_ticks(instrument_token, one_hour_ago, now, limit=1000)
    print(f"Retrieved {len(ticks)} ticks")

    print("\nFetching 1-minute candles...")
    candles_1m = get_candles_1m(instrument_token, one_hour_ago, now, interval="1min")
    print(f"Retrieved {len(candles_1m)} candles")
    print(candles_1m.head())
    
    print("\nFetching 5-minute candles...")
    candles_5m = get_candles_5m(instrument_token,one_hour_ago, now,  interval="5min")
    print(f"Retrieved {len(candles_5m)} candles")
    print(candles_5m.head())

    
    print("\nGetting order book metrics...")
  #  ob_metrics = get_orderbook_metrics(instrument_token, one_hour_ago, now)
  #  print(f"Retrieved {len(ob_metrics)} order book snapshots")

    ob_metrics = get_orderbook_metrics(4353, one_hour_ago, now)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    print(ob_metrics.to_string())
    if not ob_metrics.empty:
        print(ob_metrics.head())

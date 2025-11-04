import datetime
from src.clickhouse.clickhouse_client import get_clickhouse_client


client = get_clickhouse_client()

def insert_ticks(ticks):
    # ticks = list of dicts or tuples
    client.insert(
        "ticks_raw",
        [
            (
                t["ts"],
                t["instrument_token"],
                t["avg_traded_price"],
                t["last_price"],
                t["volume"],
                t["buy_qty"],
                t["sell_qty"]
            )
            for t in ticks
        ],
        column_names=[
            "ts",
            "instrument_token",
            "avg_traded_price",
            "last_price",
            "volume",
            "buy_qty",
            "sell_qty"
        ],
    )

def insert_snapshots(ts: datetime.datetime, b:dict):
    # ticks = list of dicts or tuples
    client.insert(
        "orderbook_snapshot",
        [
            (
                ts,
                k,
                v["best_bid"],
                v["best_ask"],
                v["imbalance_ratio"],
                v["bid_ask_spread"],
                v["mid_price"],
                v["weighted_mid_price"],
                v["order_count_imbalance"],
                v["depth_json"]

            )
            for k, v in b.items()
        ],
        column_names=[
            "ts",
            "instrument_token",
            "best_bid",
            "best_ask",
            "imbalance_ratio",
            "bid_ask_spread",
            "mid_price",
            "weighted_mid_price",
            "order_count_imbalance",
            "depth_json"
        ],
    )

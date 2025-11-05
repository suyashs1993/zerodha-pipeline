"""
clickhouse_client.py
Centralized ClickHouse client manager.
Ensures all modules use the same connection setup.
"""

import os
import clickhouse_connect
from functools import lru_cache


@lru_cache(maxsize=1)
def get_clickhouse_client():
    """
    Returns a shared ClickHouse client.
    The connection is cached so all imports reuse the same client.
    """
    #host = 'localhost'
    host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    port = int(os.getenv("CLICKHOUSE_PORT", 8123))
    user = os.getenv("CLICKHOUSE_USER", "default")
    password = os.getenv("CLICKHOUSE_PASSWORD", "")
    database = os.getenv("CLICKHOUSE_DB", "zerodhadata")

    print(f"🔌 Connecting to ClickHouse:")

    client = clickhouse_connect.get_client(
        host=host,
        port=port,
        username=user,
        password=password,
        database=database
    )

    print("✅ ClickHouse client initialized.")
    return client

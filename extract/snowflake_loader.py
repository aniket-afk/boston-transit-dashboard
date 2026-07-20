"""Loads extracted MBTA records into RAW.MBTA in Snowflake.

Its ONLY job is to write data to the warehouse. It knows nothing about the
MBTA API. Two write modes, matching the nature of each dataset:
  - append   : predictions (immutable time-series events; keep history)
  - overwrite: routes, stops, schedules (dimensions/current state)

Audit columns (loaded_at) are populated by Snowflake itself, so the timestamp
reflects when the row truly landed, on one clock.
"""
from __future__ import annotations

import logging
import os
from typing import Any

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

logger = logging.getLogger(__name__)

RAW_DATABASE = "RAW"
RAW_SCHEMA = "MBTA"


def _load_private_key() -> bytes:
    """Read the PEM private key and return it as DER bytes for the connector."""
    from cryptography.hazmat.primitives import serialization

    key_path = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]
    with open(key_path, "rb") as f:
        p_key = serialization.load_pem_private_key(f.read(), password=None)
    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def get_connection() -> snowflake.connector.SnowflakeConnection:
    """Open a Snowflake connection using the same key-pair auth dbt uses."""
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=_load_private_key(),
        role="TRANSFORMER",
        warehouse="TRANSFORMING_WH",
        database=RAW_DATABASE,
        schema=RAW_SCHEMA,
    )
    # Explicitly pin session context. The connect params above sometimes don't
    # "stick" (esp. with a suspended warehouse), so we set them directly.
    cur = conn.cursor()
    cur.execute("USE ROLE TRANSFORMER")
    cur.execute("USE WAREHOUSE TRANSFORMING_WH")
    cur.execute(f"USE DATABASE {RAW_DATABASE}")
    cur.execute(f"USE SCHEMA {RAW_DATABASE}.{RAW_SCHEMA}")
    cur.close()
    return conn


def load_records(
    records: list[dict[str, Any]],
    table: str,
    mode: str = "append",
    add_loaded_at: bool = True,
) -> int:
    """Write a list of dicts to RAW.MBTA.<table>.

    mode='append'    -> add rows, keep existing (predictions / history)
    mode='overwrite' -> replace the whole table (dimensions)
    """
    if not records:
        logger.warning("No records for %s; skipping load.", table)
        return 0

    if mode not in {"append", "overwrite"}:
        raise ValueError(f"mode must be 'append' or 'overwrite', got {mode!r}")

    df = pd.DataFrame(records)
    df.columns = [c.upper() for c in df.columns]  # Snowflake convention

    with get_connection() as conn:
        success, n_chunks, n_rows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table.upper(),
            database=RAW_DATABASE,
            schema=RAW_SCHEMA,
            auto_create_table=True,
            overwrite=(mode == "overwrite"),
            quote_identifiers=False,
        )
        if add_loaded_at:
            conn.cursor().execute(
                f'ALTER TABLE {RAW_DATABASE}.{RAW_SCHEMA}.{table.upper()} '
                f'ADD COLUMN IF NOT EXISTS LOADED_AT TIMESTAMP_NTZ'
            )
            conn.cursor().execute(
                f'UPDATE {RAW_DATABASE}.{RAW_SCHEMA}.{table.upper()} '
                f'SET LOADED_AT = CURRENT_TIMESTAMP() WHERE LOADED_AT IS NULL'
            )

    logger.info("Loaded %d rows into %s.%s.%s (mode=%s)",
                n_rows, RAW_DATABASE, RAW_SCHEMA, table.upper(), mode)
    return n_rows

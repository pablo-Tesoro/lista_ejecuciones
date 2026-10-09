"""
Watermark del proceso en process_control. Sólo lo escribe la transformación;
la ingesta lo lee para saber qué fotos de Bronze siguen sin consolidar.
"""
from __future__ import annotations

from .constants import PROCESS_CONTROL_TABLE, PROCESS_ID

NO_WATERMARK = "TIMESTAMP'1900-01-01 00:00:00'"


def ts_literal(value) -> str:
    return f"TIMESTAMP'{value.strftime('%Y-%m-%d %H:%M:%S.%f')}'"


def watermark_literal(value) -> str:
    return ts_literal(value) if value else NO_WATERMARK


def read_watermark(spark, catalog: str):
    """last_processed_ts del proceso, o None si nunca se ha consolidado nada."""
    row = spark.sql(f"""
        SELECT MAX(last_processed_ts) AS wm
          FROM {catalog}.{PROCESS_CONTROL_TABLE}
         WHERE process_id = '{PROCESS_ID}'
    """).first()
    return row["wm"] if row else None

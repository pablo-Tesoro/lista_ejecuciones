"""
Helpers compartidos de crawlers_pt.

ESTE FICHERO ES UN SUSTITUTO. En el repo real este módulo ya existe y lo
comparten todos los procesos de crawlers_pt; si es así, borra este fichero y
quédate con el vuestro: las llamadas son idénticas a las que hace
`debtors_at_ingestion`. Está aquí sólo para poder ejecutar y probar la
ingesta de executions_list fuera del repo.

API que consume la ingesta:
    get_args(kwargs)                               -> namespace con los parámetros
    read_config(step, filename)                    -> dict por entorno
    get_last_timestamp_processed(process_id, catalog, table_name)
    delete_records_after_ingestion_timestamp(catalog, table_name, ingestion_timestamp)
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path


def get_args(kwargs: list[str]):
    """Lee los parámetros de la tarea como argumentos de línea de comandos."""
    parser = argparse.ArgumentParser()
    for name in kwargs:
        parser.add_argument(f"--{name}", required=True)
    args, _unknown = parser.parse_known_args()
    return args


def read_config(step: str, filename: str) -> dict:
    """
    Carga el JSON de configuración del paso.

    Convención asumida: <paquete de crawlers_pt>/<step>/config/<filename>.
    Confirma la ruta real del repo antes de usarlo en serio.
    """
    path = Path(__file__).resolve().parents[1] / step / "config" / filename
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def get_last_timestamp_processed(process_id: str, catalog: str, table_name: str):
    """
    Último instante consolidado del proceso, o None si nunca se ha ejecutado.
    Fuera de Databricks devuelve None, que es el caso "nada consolidado".
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if spark is None:
        return None

    row = spark.sql(f"""
        SELECT MAX(last_processed_ts) AS last_processed_ts
          FROM {catalog}.{table_name.strip()}
         WHERE process_id = '{process_id}'
    """).first()
    return row["last_processed_ts"] if row else None


def delete_records_after_ingestion_timestamp(catalog: str, table_name: str,
                                             ingestion_timestamp) -> None:
    """
    Borra de la tabla las filas posteriores al instante indicado: lo que una
    ingesta anterior dejó escrito y ninguna transformación llegó a consolidar.
    Sin instante, se considera que no hay nada consolidado.
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if spark is None:
        return

    if ingestion_timestamp is None:
        limit = "TIMESTAMP'1900-01-01 00:00:00'"
    else:
        limit = f"TIMESTAMP'{ingestion_timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')}'"

    spark.sql(f"DELETE FROM {catalog}.{table_name.strip()} WHERE dp_update_ts > {limit}")

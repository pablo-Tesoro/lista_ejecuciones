"""
Lógica de la publicación en Gold de executions_list. El punto de entrada es
`executions_list_gold.py`; aquí está lo que hace, para poder probarlo por
separado.

Gold expone el estado vigente de Silver para el screening, con las columnas en
camelCase. No crea tablas: las provisiona el script de setup (sql/gold.sql).

Pasos:
  1. Dos vistas temporales desde Silver, sólo con las versiones vigentes y las
     columnas en camelCase: execution_list y letter_control.
  2. Controles de calidad sobre esas vistas. Cada uno cuenta las filas (o
     letras) que lo incumplen y tiene una severidad: Critical o Warning.
  3. Los resultados se escriben en la tabla de salida de DQ, con la decisión.
  4. Sólo si no ha fallado ningún Critical se publica, con INSERT OVERWRITE,
     que en Delta es atómico. Si falla alguno, no se publica, Gold se queda en
     su último estado bueno y el proceso termina con error.

Lo publicado es lo que se comprobó porque Silver no cambia entre los controles
y la publicación: sólo la transformación escribe Silver y el job la ejecuta
antes que Gold, nunca a la vez. Las vistas no se cachean (serverless no lo
permite); en su lugar, un control Critical más comprueba que la versión de las
tablas de Silver no ha cambiado mientras se hacían los controles.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import functions as F

from .constants import (
    DQ_RESULTS_TABLE,
    GOLD_LETTER_CONTROL_TABLE,
    GOLD_TABLE,
    LETTER_CONTROL_TABLE,
    PROCESS_ID,
    SILVER_TABLE,
)

logger = logging.getLogger("dataplatform_tenders.crawlers_pt.executions_list_gold")

CRITICAL = "Critical"
WARNING = "Warning"

# Caída máxima de filas respecto a lo que hay publicado en Gold: más es casi
# seguro un error aguas arriba, no bajas reales de un día para otro.
MAX_ROW_DROP = 0.20

V_EXECUTION_LIST = "_gold_execution_list"
V_LETTER_CONTROL = "_gold_letter_control"

GOLD_COLUMNS = (
    "caseNumber, name, debtAmount, reason, letter, court, orgUnit, "
    "enforcementAgent, terminationDate, inclusionDate, validFromDate"
)
GOLD_CONTROL_COLUMNS = "letter, totalRecords, validFromDate, lastVerifiedTs"
KEY = "caseNumber, name, debtAmount, reason"

DQ_RESULTS_DDL = (
    "process_id STRING, check_name STRING, severity STRING, target_table STRING, "
    "status STRING, failed_rows BIGINT, checked_rows BIGINT, description STRING, "
    "source_snapshot STRING, decision STRING"
)


# ==========================================================
# CONSTRUCTORES DE SQL (separados para poder probarlos sobre temp views)
# ==========================================================
def sql_execution_list_view(silver_table: str) -> str:
    """Versiones vigentes de execution_list, en camelCase."""
    return f"""
        SELECT case_number       AS caseNumber,
               name,
               debt_amount       AS debtAmount,
               reason,
               letter,
               court,
               org_unit          AS orgUnit,
               enforcement_agent AS enforcementAgent,
               termination_date  AS terminationDate,
               inclusion_date    AS inclusionDate,
               valid_from_date   AS validFromDate
          FROM {silver_table}
         WHERE is_current
    """


def sql_letter_control_view(control_table: str) -> str:
    """Total vigente de cada letra, en camelCase."""
    return f"""
        SELECT letter,
               total_records    AS totalRecords,
               valid_from_date  AS validFromDate,
               last_verified_ts AS lastVerifiedTs
          FROM {control_table}
         WHERE is_current
    """


def sql_publish(gold_table: str, view: str, columns: str) -> str:
    """Sustituye el contenido de la tabla de Gold de forma atómica."""
    return f"""
        INSERT OVERWRITE {gold_table} ({columns}, dp_update_ts)
        SELECT {columns}, current_timestamp() FROM {view}
    """


# ==========================================================
# CONTROLES DE CALIDAD
# ==========================================================
@dataclass(frozen=True)
class DqCheck:
    name: str
    severity: str
    target_table: str
    description: str
    failed_sql: str    # columna n: filas (o letras) que incumplen el control
    checked_sql: str   # columna n: filas (o letras) evaluadas


def _count(view: str) -> str:
    return f"SELECT COUNT(*) AS n FROM {view}"


def build_checks(gold_table: str, gold_control_table: str) -> list[DqCheck]:
    el, lc = V_EXECUTION_LIST, V_LETTER_CONTROL
    return [
        DqCheck(
            "execution_list_not_empty", CRITICAL, gold_table,
            "There are current records to publish",
            f"SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS n FROM {el}",
            _count(el),
        ),
        DqCheck(
            "execution_list_unique_key", CRITICAL, gold_table,
            f"One current record per business key ({KEY})",
            f"""SELECT COALESCE(SUM(c - 1), 0) AS n
                  FROM (SELECT COUNT(*) AS c FROM {el} GROUP BY {KEY} HAVING COUNT(*) > 1)""",
            _count(el),
        ),
        DqCheck(
            "execution_list_row_drop", CRITICAL, gold_table,
            f"Rows to publish are not more than {MAX_ROW_DROP:.0%} below the rows in Gold",
            f"""SELECT CASE WHEN g.n > 0 AND v.n < g.n * {1 - MAX_ROW_DROP} THEN 1 ELSE 0 END AS n
                  FROM (SELECT COUNT(*) AS n FROM {el}) v
                 CROSS JOIN (SELECT COUNT(*) AS n FROM {gold_table}) g""",
            _count(el),
        ),
        DqCheck(
            "letter_control_unique_letter", CRITICAL, gold_control_table,
            "One current total per letter",
            f"""SELECT COALESCE(SUM(c - 1), 0) AS n
                  FROM (SELECT COUNT(*) AS c FROM {lc} GROUP BY letter HAVING COUNT(*) > 1)""",
            _count(lc),
        ),
        DqCheck(
            "execution_list_letter_in_control", WARNING, gold_table,
            "Every letter with records has a current total in letter_control",
            f"""SELECT COUNT(DISTINCT e.letter) AS n
                  FROM {el} e LEFT ANTI JOIN {lc} c ON e.letter = c.letter""",
            f"SELECT COUNT(DISTINCT letter) AS n FROM {el}",
        ),
        DqCheck(
            "letter_control_total_covers_records", WARNING, gold_control_table,
            "No letter has more current records than its total in the site",
            f"""SELECT COUNT(*) AS n
                  FROM (SELECT letter, COUNT(*) AS records FROM {el} GROUP BY letter) e
                  JOIN {lc} c ON e.letter = c.letter
                 WHERE e.records > c.totalRecords""",
            _count(lc),
        ),
        DqCheck(
            "execution_list_inclusion_date", WARNING, gold_table,
            "Every record has an inclusion date (NULL: the site date could not be parsed)",
            f"SELECT COUNT(*) AS n FROM {el} WHERE inclusionDate IS NULL",
            _count(el),
        ),
    ]


def _result(check_name: str, severity: str, target_table: str, description: str,
            failed: int, checked: int) -> dict:
    status = "FAIL" if failed else "PASS"
    log = logger.warning if failed else logger.info
    log("DQ [%s] %s: %s (failed=%s, checked=%s)", severity, check_name, status, failed, checked)
    return {
        "check_name": check_name, "severity": severity, "target_table": target_table,
        "status": status, "failed_rows": failed, "checked_rows": checked,
        "description": description,
    }


def run_checks(spark, checks: list[DqCheck]) -> list[dict]:
    return [
        _result(c.name, c.severity, c.target_table, c.description,
                int(spark.sql(c.failed_sql).first()["n"]),
                int(spark.sql(c.checked_sql).first()["n"]))
        for c in checks
    ]


def latest_versions(spark, tables: list[str]) -> dict[str, int]:
    """Última versión Delta de cada tabla."""
    return {t: int(spark.sql(f"DESCRIBE HISTORY {t} LIMIT 1").first()["version"])
            for t in tables}


def write_dq_results(spark, dq_table: str, results: list[dict], *, decision: str,
                     source_snapshot: str) -> None:
    """Una fila por control. Todas comparten dp_update_ts, que identifica la ejecución."""
    rows = [
        (PROCESS_ID, r["check_name"], r["severity"], r["target_table"], r["status"],
         r["failed_rows"], r["checked_rows"], r["description"], source_snapshot, decision)
        for r in results
    ]
    (spark.createDataFrame(rows, schema=DQ_RESULTS_DDL)
        .withColumn("dp_update_ts", F.current_timestamp())
        .write.format("delta").mode("append").saveAsTable(dq_table))


# ==========================================================
# PUBLICACIÓN
# ==========================================================
def run_gold(spark, *, catalog: str) -> dict:
    silver = f"{catalog}.{SILVER_TABLE}"
    control = f"{catalog}.{LETTER_CONTROL_TABLE}"
    gold = f"{catalog}.{GOLD_TABLE}"
    gold_control = f"{catalog}.{GOLD_LETTER_CONTROL_TABLE}"

    versions = latest_versions(spark, [silver, control])
    source_snapshot = ";".join(f"{t}@v{v}" for t, v in versions.items())
    logger.info("Silver snapshot: %s", source_snapshot)

    # 1. Vistas
    spark.sql(f"CREATE OR REPLACE TEMP VIEW {V_EXECUTION_LIST} AS "
              + sql_execution_list_view(silver))
    spark.sql(f"CREATE OR REPLACE TEMP VIEW {V_LETTER_CONTROL} AS "
              + sql_letter_control_view(control))

    # 2. Controles
    results = run_checks(spark, build_checks(gold, gold_control))
    unchanged = latest_versions(spark, [silver, control]) == versions
    results.append(_result(
        "silver_unchanged_during_checks", CRITICAL, gold,
        "Silver did not change while the checks ran, so what is published is what was checked",
        0 if unchanged else 1, len(versions)))

    failed_critical = [r["check_name"] for r in results
                       if r["severity"] == CRITICAL and r["status"] == "FAIL"]
    failed_warning = [r["check_name"] for r in results
                      if r["severity"] == WARNING and r["status"] == "FAIL"]
    decision = "block" if failed_critical else "publish"

    # 3. Resultados de DQ, antes de publicar
    write_dq_results(spark, f"{catalog}.{DQ_RESULTS_TABLE}", results,
                     decision=decision, source_snapshot=source_snapshot)

    if failed_critical:
        raise RuntimeError(
            f"Gold not published, it keeps its last good state. "
            f"Critical DQ checks failed: {failed_critical}")

    # 4. Publicación
    spark.sql(sql_publish(gold, V_EXECUTION_LIST, GOLD_COLUMNS))
    spark.sql(sql_publish(gold_control, V_LETTER_CONTROL, GOLD_CONTROL_COLUMNS))

    by_name = {r["check_name"]: r for r in results}
    summary = {
        "decision": decision,
        "execution_list_rows": by_name["execution_list_not_empty"]["checked_rows"],
        "letter_control_rows": by_name["letter_control_unique_letter"]["checked_rows"],
        "failed_warning": failed_warning,
        "source_snapshot": source_snapshot,
    }
    logger.info("Gold summary: %s", summary)
    return summary

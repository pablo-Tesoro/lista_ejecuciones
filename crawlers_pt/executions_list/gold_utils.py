"""
Lógica de la publicación en Gold de executions_list. El punto de entrada es
`executions_list_gold.py`; aquí está lo que hace, para poder probarlo por
separado.

Gold expone sólo el estado vigente de Silver, en camelCase (dp_update_ts se
mantiene tal cual), y se reconstruye entero en cada ejecución. No crea tablas:
las provisiona el script de setup con sql/gold.sql.

Pasos:
  1. Dos vistas temporales desde Silver, sólo con las versiones vigentes y las
     columnas de Gold: execution_list y letter_control.
  2. Controles de calidad sobre esas vistas, cada uno Critical o Warning. Unity
     Catalog no valida las PK ni la FK declaradas en Gold, así que las validan
     controles Critical. Los resultados van al log.
  3. Sólo si no ha fallado ningún Critical se publica, con INSERT OVERWRITE,
     que en Delta es atómico. Si falla alguno, no se publica, Gold se queda en
     su último estado bueno y el proceso termina con error.

Lo publicado es lo que se comprobó porque Silver no cambia entre los controles
y la publicación: sólo la transformación escribe Silver y el job la ejecuta
antes que Gold, nunca a la vez. Las vistas no se cachean (serverless no lo
permite); en su lugar, un control Critical comprueba que la versión de las
tablas de Silver no ha cambiado mientras se hacían los controles.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

from .constants import GOLD_LETTER_CONTROL_TABLE, GOLD_TABLE, LETTER_CONTROL_TABLE, SILVER_TABLE

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
    "enforcementAgent, terminationDate, inclusionDate, rowHash"
)
GOLD_CONTROL_COLUMNS = "letter, totalRecords, lastVerifiedTs"
KEY = "caseNumber, name, debtAmount, reason"


# ==========================================================
# CONSTRUCTORES DE SQL (separados para poder probarlos sobre temp views)
# ==========================================================
def sql_execution_list_view(silver_table: str) -> str:
    """Versiones vigentes de execution_list con las columnas de Gold."""
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
               row_hash          AS rowHash
          FROM {silver_table}
         WHERE is_current
    """


def sql_letter_control_view(control_table: str) -> str:
    """Total vigente de cada letra con las columnas de Gold."""
    return f"""
        SELECT letter,
               total_records    AS totalRecords,
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
    expectation: str
    failed_sql: str    # columna n: filas (o letras) que incumplen el control
    checked_sql: str   # columna n: filas (o letras) evaluadas


@dataclass(frozen=True)
class DqResult:
    name: str
    severity: str
    failed: int
    checked: int


def _count(view: str) -> str:
    return f"SELECT COUNT(*) AS n FROM {view}"


def build_checks(gold_table: str) -> list[DqCheck]:
    el, lc = V_EXECUTION_LIST, V_LETTER_CONTROL
    return [
        DqCheck(
            "execution_list_not_empty", CRITICAL,
            "there are current records to publish",
            f"SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END AS n FROM {el}",
            _count(el),
        ),
        DqCheck(
            "execution_list_pk", CRITICAL,
            f"PK of execution_list: one current record per ({KEY})",
            f"""SELECT COALESCE(SUM(c - 1), 0) AS n
                  FROM (SELECT COUNT(*) AS c FROM {el} GROUP BY {KEY} HAVING COUNT(*) > 1)""",
            _count(el),
        ),
        DqCheck(
            "letter_control_pk", CRITICAL,
            "PK of letter_control: one current total per letter",
            f"""SELECT COALESCE(SUM(c - 1), 0) AS n
                  FROM (SELECT COUNT(*) AS c FROM {lc} GROUP BY letter HAVING COUNT(*) > 1)""",
            _count(lc),
        ),
        DqCheck(
            "execution_list_letter_fk", CRITICAL,
            "FK execution_list.letter -> letter_control.letter: every letter with records "
            "has its current total",
            f"""SELECT COUNT(DISTINCT e.letter) AS n
                  FROM {el} e LEFT ANTI JOIN {lc} c ON e.letter = c.letter""",
            f"SELECT COUNT(DISTINCT letter) AS n FROM {el}",
        ),
        DqCheck(
            "execution_list_row_drop", CRITICAL,
            f"rows to publish are not more than {MAX_ROW_DROP:.0%} below the rows in Gold",
            f"""SELECT CASE WHEN g.n > 0 AND v.n < g.n * {1 - MAX_ROW_DROP} THEN 1 ELSE 0 END AS n
                  FROM (SELECT COUNT(*) AS n FROM {el}) v
                 CROSS JOIN (SELECT COUNT(*) AS n FROM {gold_table}) g""",
            _count(el),
        ),
        DqCheck(
            "letter_control_total_covers_records", WARNING,
            "no letter has more current records than its total in the site",
            f"""SELECT COUNT(*) AS n
                  FROM (SELECT letter, COUNT(*) AS records FROM {el} GROUP BY letter) e
                  JOIN {lc} c ON e.letter = c.letter
                 WHERE e.records > c.totalRecords""",
            _count(lc),
        ),
        DqCheck(
            "execution_list_inclusion_date", WARNING,
            "every record has an inclusion date (NULL: the site date could not be parsed)",
            f"SELECT COUNT(*) AS n FROM {el} WHERE inclusionDate IS NULL",
            _count(el),
        ),
    ]


def _result(name: str, severity: str, expectation: str, failed: int, checked: int) -> DqResult:
    if failed:
        logger.warning("DQ [%s] %s: FAIL (failed=%s, checked=%s). Expected: %s",
                       severity, name, failed, checked, expectation)
    else:
        logger.info("DQ [%s] %s: PASS (checked=%s)", severity, name, checked)
    return DqResult(name, severity, failed, checked)


def run_checks(spark, checks: list[DqCheck]) -> list[DqResult]:
    return [
        _result(c.name, c.severity, c.expectation,
                int(spark.sql(c.failed_sql).first()["n"]),
                int(spark.sql(c.checked_sql).first()["n"]))
        for c in checks
    ]


def latest_versions(spark, tables: list[str]) -> dict[str, int]:
    """Última versión Delta de cada tabla."""
    return {t: int(spark.sql(f"DESCRIBE HISTORY {t} LIMIT 1").first()["version"])
            for t in tables}


# ==========================================================
# PUBLICACIÓN
# ==========================================================
def run_gold(spark, *, catalog: str) -> dict:
    silver = f"{catalog}.{SILVER_TABLE}"
    control = f"{catalog}.{LETTER_CONTROL_TABLE}"
    gold = f"{catalog}.{GOLD_TABLE}"
    gold_control = f"{catalog}.{GOLD_LETTER_CONTROL_TABLE}"

    versions = latest_versions(spark, [silver, control])
    logger.info("Silver snapshot: %s", ", ".join(f"{t} v{v}" for t, v in versions.items()))

    # 1. Vistas
    spark.sql(f"CREATE OR REPLACE TEMP VIEW {V_EXECUTION_LIST} AS "
              + sql_execution_list_view(silver))
    spark.sql(f"CREATE OR REPLACE TEMP VIEW {V_LETTER_CONTROL} AS "
              + sql_letter_control_view(control))

    # 2. Controles
    results = run_checks(spark, build_checks(gold))
    unchanged = latest_versions(spark, [silver, control]) == versions
    results.append(_result(
        "silver_unchanged_during_checks", CRITICAL,
        "Silver does not change while the checks run",
        0 if unchanged else 1, len(versions)))

    failed_critical = [r.name for r in results if r.severity == CRITICAL and r.failed]
    failed_warning = [r.name for r in results if r.severity == WARNING and r.failed]
    if failed_critical:
        raise RuntimeError(
            f"Gold not published, it keeps its last good state. "
            f"Critical DQ checks failed: {failed_critical}")

    # 3. Publicación. letter_control primero: así la FK de execution_list se
    #    cumple también entre las dos escrituras.
    spark.sql(sql_publish(gold_control, V_LETTER_CONTROL, GOLD_CONTROL_COLUMNS))
    spark.sql(sql_publish(gold, V_EXECUTION_LIST, GOLD_COLUMNS))

    by_name = {r.name: r for r in results}
    summary = {
        "execution_list_rows": by_name["execution_list_not_empty"].checked,
        "letter_control_rows": by_name["letter_control_pk"].checked,
        "failed_warning": failed_warning,
    }
    logger.info("Gold published: %s", summary)
    return summary

"""
Lógica de la transformación de executions_list (Bronze -> Silver, SCD2). El
punto de entrada es `executions_list_transformation.py`; aquí está lo que
hace, para poder probarlo por separado.

Es el ÚNICO proceso que modifica Silver: execution_list, letter_control y
process_control. Todo lo que necesita saber de una carga lo deduce de Bronze.
No crea tablas: las provisiona el script de setup (model.sql).

Pasos:
  1. Watermark: last_processed_ts de process_control (sin fila -> 1900).
  2. Ventana: letras con filas en Bronze posteriores al watermark, y el
     instante de su foto. Tras la limpieza previa de la ingesta hay como mucho
     una foto por letra.
  3. Delta: filas de esas fotos, tipadas y con row_hash:
        sha2(concat_ws('|', coalesce(f1,''), ..., coalesce(f9,'')), 256)
     la misma fórmula que usa la ingesta en Python (row_hash_from_values).
  4. Claves incompletas: si el importe no se pudo tipar, la fila no tiene
     clave completa y se excluye, contándola en el log.
  5. Deduplicación por clave de negocio (case_number, name, debt_amount,
     reason): el MERGE de Delta rechaza fuentes con claves repetidas.
  6. execution_list, en tres escrituras:
       · misma clave y mismo row_hash -> nada
       · misma clave y row_hash distinto -> se cierra la vigente y se inserta
         la nueva versión
       · clave sin versión vigente -> alta
       · clave vigente de una letra de la ventana ausente de su foto -> cierre
         por desaparición, salvo que estuviera entre las claves incompletas
         (un fallo de parseo no es una baja)
     Nunca se modifica un atributo in situ: una versión vigente tiene
     valid_to_date a NULL.
  7. letter_control, en dos escrituras. El total de una letra es el número de
     filas de su foto, que coincide con el contador de la web porque la ingesta
     sólo escribe la letra si cuadran:
       · total distinto -> se cierra la vigente y se abre otra
       · mismo total -> sólo se refresca last_verified_ts
       · sin vigente -> primera carga
  8. Watermark = MAX(dp_update_ts) de todo Bronze. Sólo avanza. Es seguro
     mientras ingesta y transformación no se ejecuten a la vez.

Idempotente: si muere antes de mover el watermark, la siguiente ejecución
repite la ventana, encuentra las versiones ya vigentes con el mismo row_hash
y el mismo total, y no hace nada nuevo.

Limitación asumida: una letra que se queda a 0 registros no escribe filas en
Bronze, así que no entra en la ventana y sus registros no se cierran.
"""
from __future__ import annotations

import logging

from .constants import (
    BRONZE_TABLE,
    LETTER_CONTROL_TABLE,
    PROCESS_CONTROL_TABLE,
    PROCESS_ID,
    SILVER_TABLE,
)
from .process_control import read_watermark, ts_literal, watermark_literal

logger = logging.getLogger("dataplatform_tenders.crawlers_pt.executions_list_transformation")

# Orden del hash canónico: el mismo que FIELD_MAP en la ingesta.
HASH_FIELDS = [
    "name", "debt_amount", "case_number", "court", "org_unit",
    "enforcement_agent", "termination_date", "inclusion_date", "reason",
]

# Clave de negocio: las cuatro columnas son NOT NULL en Silver.
KEY_COLS = ["case_number", "name", "debt_amount", "reason"]

DELTA_COLUMNS = (
    "case_number, name, debt_amount, reason, letter, court, org_unit, "
    "enforcement_agent, termination_date, inclusion_date, "
    "page_number, position_in_page, row_hash, snapshot_ts"
)

SILVER_COLUMNS = (
    "case_number, name, debt_amount, reason, letter, court, org_unit, "
    "enforcement_agent, termination_date, inclusion_date, "
    "page_number, position_in_page, row_hash, "
    "valid_from_date, valid_to_date, is_current, dp_update_ts"
)

CONTROL_COLUMNS = (
    "letter, total_records, valid_from_date, valid_to_date, "
    "last_verified_ts, is_current, dp_update_ts"
)

OPEN = "CAST(NULL AS TIMESTAMP)"   # valid_to_date de una versión vigente

V_PROCESSED = "_executions_processed"
V_DELTA = "_executions_delta"
V_TOTALS = "_executions_totals"
V_INCOMPLETE = "_executions_incomplete"
V_DEDUP = "_executions_dedup"
V_CLOSES = "_executions_closes"


def key_join(a: str, b: str) -> str:
    """Igualdad simple por clave: sql_delta_dedup garantiza que no hay NULL."""
    return " AND ".join(f"{a}.{c} = {b}.{c}" for c in KEY_COLS)


# ==========================================================
# CONSTRUCTORES DE SQL (separados para poder probarlos sobre temp views)
# ==========================================================
def sql_processed_letters(bronze_table: str, watermark: str) -> str:
    """
    Letras con filas en Bronze posteriores al watermark, y el instante de su
    foto. El MAX es sólo una defensa por si se ejecutara sin la limpieza previa.
    """
    return f"""
        SELECT letter, MAX(dp_update_ts) AS snapshot_ts
          FROM {bronze_table}
         WHERE dp_update_ts > {watermark}
         GROUP BY letter
    """


def sql_delta_typed(bronze_table: str, processed_view: str) -> str:
    """
    Filas de Bronze de la ventana, tipadas y con row_hash.

      importe: quita €, espacios y NBSP; los puntos de millar sólo si hay
               coma decimal; coma -> punto; no parseable -> NULL.
      fechas : dd-MM-yyyy o dd/MM/yyyy -> DATE; el resto -> NULL.
    El valor crudo siempre permanece en Bronze.
    """
    cleaned = "replace(replace(replace(b.debt_amount, '€', ''), chr(160), ''), ' ', '')"
    debt_cast = f"""
        CASE WHEN instr(b.debt_amount, ',') > 0
             THEN try_cast(replace(replace({cleaned}, '.', ''), ',', '.') AS DECIMAL(23,8))
             ELSE try_cast({cleaned} AS DECIMAL(23,8))
        END
    """
    hash_expr = ("sha2(concat_ws('|', "
                 + ", ".join(f"coalesce(b.{c}, '')" for c in HASH_FIELDS)
                 + "), 256)")

    def date_cast(col: str) -> str:
        return f"CAST(try_to_timestamp(replace(b.{col}, '/', '-'), 'dd-MM-yyyy') AS DATE)"

    return f"""
        SELECT
            b.case_number,
            b.name,
            {debt_cast} AS debt_amount,
            b.reason,
            b.letter,
            b.court,
            b.org_unit,
            b.enforcement_agent,
            {date_cast('termination_date')} AS termination_date,
            {date_cast('inclusion_date')} AS inclusion_date,
            b.page_number,
            b.position_in_page,
            {hash_expr} AS row_hash,
            p.snapshot_ts
        FROM {bronze_table} b
        JOIN {processed_view} p
          ON b.letter = p.letter AND b.dp_update_ts = p.snapshot_ts
    """


def sql_key_incomplete(delta_view: str) -> str:
    """
    Filas a las que les falta alguna columna de la clave. El caso real es
    debt_amount: try_cast devuelve NULL cuando el importe no es parseable.
    """
    cond = " OR ".join(f"d.{c} IS NULL" for c in KEY_COLS)
    return f"SELECT * FROM {delta_view} d WHERE {cond}"


def sql_delta_dedup(delta_view: str) -> str:
    """
    Una fila por clave de negocio, con la clave completa: descarta las claves
    incompletas y, entre repetidas, conserva la primera aparición en el listado.
    """
    order = ", ".join(KEY_COLS)
    not_null = " AND ".join(f"d.{c} IS NOT NULL" for c in KEY_COLS)
    return f"""
        SELECT {DELTA_COLUMNS} FROM (
            SELECT d.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY {order}
                       ORDER BY d.page_number, d.position_in_page
                   ) AS rn
              FROM {delta_view} d
             WHERE {not_null}
        ) WHERE rn = 1
    """


def sql_scd_merge(silver_table: str, dedup_view: str) -> str:
    """
    Primera escritura de execution_list, emparejando por clave + is_current:
      mismo row_hash    -> no se toca nada
      row_hash distinto -> cierra la versión vigente
      sin vigente       -> alta
    Las versiones nuevas de las cerradas las inserta sql_insert_new_versions.
    """
    return f"""
        MERGE INTO {silver_table} AS t
        USING {dedup_view} AS s
           ON {key_join('t', 's')} AND t.is_current
        WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET
            t.valid_to_date = s.snapshot_ts,
            t.is_current    = false,
            t.dp_update_ts  = current_timestamp()
        WHEN NOT MATCHED THEN INSERT ({SILVER_COLUMNS})
        VALUES (
            s.case_number, s.name, s.debt_amount, s.reason, s.letter,
            s.court, s.org_unit, s.enforcement_agent,
            s.termination_date, s.inclusion_date,
            s.page_number, s.position_in_page, s.row_hash,
            s.snapshot_ts, {OPEN}, true, current_timestamp()
        )
    """


def sql_insert_new_versions(silver_table: str, dedup_view: str) -> str:
    """Segunda escritura: versiones nuevas de las claves que el MERGE acaba de cerrar."""
    return f"""
        INSERT INTO {silver_table} ({SILVER_COLUMNS})
        SELECT s.case_number, s.name, s.debt_amount, s.reason, s.letter,
               s.court, s.org_unit, s.enforcement_agent,
               s.termination_date, s.inclusion_date,
               s.page_number, s.position_in_page, s.row_hash,
               s.snapshot_ts, {OPEN}, true, current_timestamp()
          FROM {dedup_view} s
         WHERE NOT EXISTS (
               SELECT 1 FROM {silver_table} t
                WHERE {key_join('t', 's')} AND t.is_current
         )
    """


def sql_close_candidates(silver_table: str, processed_view: str,
                         dedup_view: str, incomplete_view: str) -> str:
    """
    Claves vigentes de las letras de la ventana ausentes de su foto.

    Se protegen las que quedaron con clave incompleta: si el importe de un
    registro deja de ser tipable, esa fila no llega al delta, y sin esta
    salvaguarda se cerraría como si hubiera desaparecido. El emparejamiento va
    por (case_number, name, reason), sin debt_amount, que es la columna que no
    se pudo construir.
    """
    cols = ", ".join(f"t.{c}" for c in KEY_COLS)
    protect = " AND ".join(f"i.{c} = t.{c}" for c in KEY_COLS if c != "debt_amount")
    return f"""
        SELECT {cols}, p.snapshot_ts
          FROM {silver_table} t
          JOIN {processed_view} p
            ON t.letter = p.letter
         WHERE t.is_current
           AND NOT EXISTS (
               SELECT 1 FROM {dedup_view} d
                WHERE {key_join('d', 't')}
           )
           AND NOT EXISTS (
               SELECT 1 FROM {incomplete_view} i
                WHERE {protect}
           )
    """


def sql_close_disappeared(silver_table: str, candidates_view: str) -> str:
    """Tercera escritura: cierre por desaparición."""
    return f"""
        MERGE INTO {silver_table} AS t
        USING {candidates_view} AS x
           ON {key_join('t', 'x')} AND t.is_current
        WHEN MATCHED THEN UPDATE SET
            t.valid_to_date = x.snapshot_ts,
            t.is_current    = false,
            t.dp_update_ts  = current_timestamp()
    """


def sql_letter_totals(delta_view: str) -> str:
    """
    Total de cada letra = filas de su foto, contadas sobre el delta completo,
    antes de excluir claves incompletas o duplicadas.
    """
    return f"""
        SELECT letter, snapshot_ts, COUNT(*) AS total_records
          FROM {delta_view}
         GROUP BY letter, snapshot_ts
    """


def sql_control_merge(control_table: str, totals_view: str) -> str:
    """
    Primera escritura de letter_control, contra la versión vigente:
      total distinto -> se cierra (la nueva la inserta sql_control_insert_new)
      mismo total    -> sólo se refresca last_verified_ts
      sin vigente    -> primera carga
    """
    return f"""
        MERGE INTO {control_table} AS t
        USING {totals_view} AS s
           ON t.letter = s.letter AND t.is_current
        WHEN MATCHED AND t.total_records <> s.total_records THEN UPDATE SET
            t.valid_to_date = s.snapshot_ts,
            t.is_current    = false,
            t.dp_update_ts  = current_timestamp()
        WHEN MATCHED AND t.total_records = s.total_records
                     AND s.snapshot_ts > t.last_verified_ts THEN UPDATE SET
            t.last_verified_ts = s.snapshot_ts,
            t.dp_update_ts     = current_timestamp()
        WHEN NOT MATCHED THEN INSERT ({CONTROL_COLUMNS})
        VALUES (s.letter, s.total_records, s.snapshot_ts, {OPEN},
                s.snapshot_ts, true, current_timestamp())
    """


def sql_control_insert_new(control_table: str, totals_view: str) -> str:
    """Segunda escritura: nueva versión de las letras cuyo total acaba de cerrarse."""
    return f"""
        INSERT INTO {control_table} ({CONTROL_COLUMNS})
        SELECT s.letter, s.total_records, s.snapshot_ts, {OPEN},
               s.snapshot_ts, true, current_timestamp()
          FROM {totals_view} s
         WHERE NOT EXISTS (
               SELECT 1 FROM {control_table} t
                WHERE t.letter = s.letter AND t.is_current
         )
    """


def sql_advance_watermark(process_table: str, process_id: str, watermark: str) -> str:
    return f"""
        MERGE INTO {process_table} AS t
        USING (SELECT '{process_id}' AS process_id,
                      {watermark} AS last_processed_ts) AS s
           ON t.process_id = s.process_id
        WHEN MATCHED THEN UPDATE SET
            t.last_processed_ts = s.last_processed_ts,
            t.dp_update_ts      = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (process_id, last_processed_ts, dp_update_ts)
            VALUES (s.process_id, s.last_processed_ts, current_timestamp())
    """


# ==========================================================
# TRANSFORMACIÓN
# ==========================================================
def _count(spark, sql: str) -> int:
    return int(spark.sql(sql).first()["n"])


def run_transformation(spark, *, catalog: str) -> dict:
    bronze = f"{catalog}.{BRONZE_TABLE}"
    silver = f"{catalog}.{SILVER_TABLE}"
    control = f"{catalog}.{LETTER_CONTROL_TABLE}"
    process = f"{catalog}.{PROCESS_CONTROL_TABLE}"

    # 1. Watermark
    wm_value = read_watermark(spark, catalog)
    watermark = watermark_literal(wm_value)
    logger.info("Watermark (process_id=%s): %s", PROCESS_ID, watermark)

    # 2. Ventana
    spark.sql(f"CREATE OR REPLACE TEMP VIEW {V_PROCESSED} AS "
              + sql_processed_letters(bronze, watermark))
    processed = spark.sql(
        f"SELECT letter, snapshot_ts FROM {V_PROCESSED} ORDER BY letter").collect()

    if not processed:
        logger.info("No rows in Bronze after the watermark: nothing to do.")
        return {"processed_letters": [], "delta_rows": 0}

    letters = [r["letter"] for r in processed]
    logger.info("Letters with a new snapshot in Bronze: %s", letters)

    # 3-5. Delta tipado, totales, claves incompletas, deduplicación, cierres
    spark.sql(f"CREATE OR REPLACE TEMP VIEW {V_DELTA} AS "
              + sql_delta_typed(bronze, V_PROCESSED))
    spark.sql(f"CREATE OR REPLACE TEMP VIEW {V_TOTALS} AS " + sql_letter_totals(V_DELTA))
    spark.sql(f"CREATE OR REPLACE TEMP VIEW {V_INCOMPLETE} AS " + sql_key_incomplete(V_DELTA))
    spark.sql(f"CREATE OR REPLACE TEMP VIEW {V_DEDUP} AS " + sql_delta_dedup(V_DELTA))
    spark.sql(f"CREATE OR REPLACE TEMP VIEW {V_CLOSES} AS "
              + sql_close_candidates(silver, V_PROCESSED, V_DEDUP, V_INCOMPLETE))

    # Contadores, calculados antes de escribir nada.
    delta_rows = _count(spark, f"SELECT COUNT(*) n FROM {V_DELTA}")
    dedup_rows = _count(spark, f"SELECT COUNT(*) n FROM {V_DEDUP}")
    incomplete = _count(spark, f"SELECT COUNT(*) n FROM {V_INCOMPLETE}")
    duplicate_keys = delta_rows - incomplete - dedup_rows
    new_versions = _count(spark, f"""
        SELECT COUNT(*) n FROM {V_DEDUP} s
         WHERE NOT EXISTS (
             SELECT 1 FROM {silver} t
              WHERE {key_join('t', 's')} AND t.is_current AND t.row_hash = s.row_hash
         )
    """)
    closes_gone = _count(spark, f"SELECT COUNT(*) n FROM {V_CLOSES}")
    totals_changed = _count(spark, f"""
        SELECT COUNT(*) n FROM {V_TOTALS} s
         WHERE NOT EXISTS (
             SELECT 1 FROM {control} t
              WHERE t.letter = s.letter AND t.is_current
                AND t.total_records = s.total_records
         )
    """)

    if incomplete:
        logger.warning("%s rows excluded from Silver: incomplete key (usually a debt amount "
                       "that cannot be typed). The raw value stays in Bronze.", incomplete)
    if duplicate_keys:
        logger.warning("%s rows dropped: business key (case_number, name, debt_amount, reason) "
                       "repeated within its snapshot.", duplicate_keys)

    # 6. execution_list
    spark.sql(sql_scd_merge(silver, V_DEDUP))
    spark.sql(sql_insert_new_versions(silver, V_DEDUP))
    spark.sql(sql_close_disappeared(silver, V_CLOSES))

    # 7. letter_control
    spark.sql(sql_control_merge(control, V_TOTALS))
    spark.sql(sql_control_insert_new(control, V_TOTALS))

    # 8. Watermark: MAX(dp_update_ts) de todo Bronze, sólo avanza.
    max_bronze = spark.sql(f"SELECT MAX(dp_update_ts) AS m FROM {bronze}").first()["m"]
    if max_bronze and (wm_value is None or max_bronze > wm_value):
        new_watermark = ts_literal(max_bronze)
        spark.sql(sql_advance_watermark(process, PROCESS_ID, new_watermark))
    else:
        new_watermark = watermark
    logger.info("Watermark: %s -> %s", watermark, new_watermark)

    summary = {
        "processed_letters": letters,
        "delta_rows": delta_rows,
        "unique_keys": dedup_rows,
        "incomplete_keys": incomplete,
        "duplicate_keys": duplicate_keys,
        "new_versions": new_versions,
        "closed_disappeared": closes_gone,
        "letters_total_changed": totals_changed,
        "new_watermark": new_watermark,
    }
    logger.info("Transformation summary: %s", summary)
    return summary

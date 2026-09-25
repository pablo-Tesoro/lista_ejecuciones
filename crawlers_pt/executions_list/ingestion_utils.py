"""
Lógica de la ingesta de executions_list. El punto de entrada es
`executions_list_ingestion.py`; aquí está lo que hace, para poder probarlo
por separado.

La ingesta SÓLO escribe Bronze: nunca crea ni modifica tablas de Silver,
sólo las lee para las comprobaciones previas. ingestion_date y dp_update_ts
los pone Spark en el momento de insertar.
"""
from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass

from pyspark.sql import functions as F

from .constants import BRONZE_SCHEMA, BRONZE_TABLE, LETTER_CONTROL_TABLE, SILVER_TABLE
from .crawler import CitiusCrawler
from .settings import CrawlerSettings

logger = logging.getLogger("dataplatform_tenders.crawlers_pt.executions_list_ingestion")

ROWS_PER_PAGE = 10

# Atributo del ParsedRecord (portugués) -> columna del modelo (inglés).
# El orden es el del hash canónico y el de las columnas de Bronze.
FIELD_MAP = [
    ("nome", "name"),
    ("valor_divida", "debt_amount"),
    ("n_processo", "case_number"),
    ("tribunal", "court"),
    ("un_org", "org_unit"),
    ("agente_execucao", "enforcement_agent"),
    ("extincao", "termination_date"),
    ("inclusao", "inclusion_date"),
    ("motivo", "reason"),
]
SITE_FIELDS = [en for _pt, en in FIELD_MAP]

# Columnas que aporta el crawl. ingestion_date y dp_update_ts NO van aquí:
# las fija Spark al insertar, como en el resto de procesos de crawlers_pt.
BRONZE_DATA_COLUMNS = [*SITE_FIELDS, "letter", "page_number", "position_in_page"]
BRONZE_COLUMNS = [*BRONZE_DATA_COLUMNS, "ingestion_date", "dp_update_ts"]
BRONZE_DATA_DDL = ", ".join(
    f"{c} INT" if c in ("page_number", "position_in_page") else f"{c} STRING"
    for c in BRONZE_DATA_COLUMNS
)


# ==========================================================
# HASH CANÓNICO
# ==========================================================
def row_hash_from_values(values) -> str:
    """
    sha256 de los 9 campos CRUDOS unidos por '|'.

    Definición deliberadamente simple para poder replicarla carácter a
    carácter en Spark SQL, que es donde la transformación calcula el
    row_hash que guarda en Silver.
    """
    payload = "|".join((v or "") for v in values)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def record_row_hash(rec) -> str:
    return row_hash_from_values(getattr(rec, pt, "") for pt, _en in FIELD_MAP)


def _truncate(parsed_rows, site_total: int):
    return parsed_rows[:site_total] if 0 <= site_total < len(parsed_rows) else parsed_rows


def first_record_hash(parsed_rows, site_total: int) -> str | None:
    """row_hash del primer registro de la página 1; None si la letra está vacía."""
    rows = _truncate(parsed_rows, site_total)
    return record_row_hash(rows[0]) if rows else None


def first_record_values(parsed_rows, site_total: int) -> tuple | None:
    """Los 9 campos crudos del primer registro, para comparar contra Bronze."""
    rows = _truncate(parsed_rows, site_total)
    if not rows:
        return None
    return tuple((getattr(rows[0], pt, "") or "") for pt, _en in FIELD_MAP)


def record_to_row(rec, *, letter: str, page_number: int, position: int) -> dict:
    row = {en: getattr(rec, pt, "") for pt, en in FIELD_MAP}
    row["letter"] = letter
    row["page_number"] = page_number
    row["position_in_page"] = position
    return row


# ==========================================================
# DESCARGA COMPLETA DE UNA LETRA
# ==========================================================
@dataclass
class LetterSnapshot:
    letter: str
    total_records: int
    total_pages: int
    first_hash: str | None
    first_values: tuple | None
    rows: list[dict]
    # >0 en una carga incremental: `rows` son sólo las altas y la foto se
    # completa arrastrando estas filas de la última foto de Bronze.
    carried_rows: int = 0


def row_values(row: dict) -> tuple:
    """Los 9 campos crudos de una fila de Bronze, en el orden canónico."""
    return tuple((row[c] or "") for c in SITE_FIELDS)


class LetterCrawl:
    """
    Descarga de una letra que puede pararse tras las primeras N filas y
    reanudarse después: las páginas ya leídas no se vuelven a pedir.
    """

    def __init__(self, crawler: CitiusCrawler, *, letter: str, page1):
        self.crawler = crawler
        self.letter = letter
        self.expected_total = int(page1.record_count or 0)

        if not page1.parsed_rows and self.expected_total > 0:
            raise RuntimeError(
                f"Letter {letter}: page 1 returned 0 rows but the counter says "
                f"{self.expected_total}."
            )

        self.rows: list[dict] = []
        self._visited: set[str] = set()
        self._current = page1
        self._page_no = 1
        self._exhausted = False
        self._consume_current()

    def _consume_current(self) -> None:
        current, page_no = self._current, self._page_no

        if current.page_signature and current.page_signature in self._visited:
            raise RuntimeError(
                f"Letter {self.letter}: pagination loop detected at page {page_no}.")
        if current.page_signature:
            self._visited.add(current.page_signature)

        if page_no > 1 and not current.parsed_rows:
            raise RuntimeError(f"Letter {self.letter}: page {page_no} returned 0 rows.")

        remaining = self.expected_total - len(self.rows)
        page_rows = current.parsed_rows[:max(remaining, 0)]
        for pos, rec in enumerate(page_rows, start=1):
            self.rows.append(
                record_to_row(rec, letter=self.letter, page_number=page_no, position=pos))

        if not current.next_page_number or len(self.rows) >= self.expected_total:
            self._exhausted = True

    def _advance(self) -> None:
        settings = self.crawler.settings
        next_page = self._current.next_page_number
        if (settings.reset_session_every_pages > 0
                and next_page % settings.reset_session_every_pages == 0):
            self.crawler.reset_session()

        self._current = self.crawler.fetch_page(
            self._current.soup, next_page, letter=self.letter)
        self._page_no = next_page

        if (settings.long_pause_every_pages > 0
                and next_page % settings.long_pause_every_pages == 0):
            time.sleep(settings.long_pause_min_s)
        else:
            self.crawler.polite_pause()

        self._consume_current()

    def fetch_until(self, n_rows: int) -> None:
        """Pagina hasta tener al menos n_rows filas o llegar al final."""
        while len(self.rows) < n_rows and not self._exhausted:
            self._advance()

    @property
    def pages_read(self) -> int:
        return self._page_no

    def finish(self) -> LetterSnapshot:
        """Completa la descarga y comprueba que cuadra con el contador."""
        self.fetch_until(self.expected_total)

        if len(self.rows) != self.expected_total:
            raise RuntimeError(
                f"Letter {self.letter}: crawled {len(self.rows)} rows but the counter said "
                f"{self.expected_total}."
            )

        first = self.rows[0] if self.rows else None
        return LetterSnapshot(
            letter=self.letter,
            total_records=self.expected_total,
            total_pages=max(1, -(-self.expected_total // ROWS_PER_PAGE)),
            first_hash=row_hash_from_values(first[c] for c in SITE_FIELDS) if first else None,
            first_values=row_values(first) if first else None,
            rows=self.rows,
        )


def crawl_letter(crawler: CitiusCrawler, *, letter: str, page1) -> LetterSnapshot:
    """Descarga la letra entera en memoria. Aborta si algo no cuadra."""
    return LetterCrawl(crawler, letter=letter, page1=page1).finish()


def crawl_letter_incremental(
    crawler: CitiusCrawler, *, letter: str, page1, previous_total: int,
    previous_first_values: tuple,
) -> LetterSnapshot:
    """
    Las altas entran por arriba del listado. Si el contador creció en k y el
    primer registro de la última foto aparece justo en la posición k+1, no hubo
    bajas: basta con descargar las k altas y arrastrar la foto anterior.

    Si no aparece en esa posición (hubo bajas, o un alta no entró por arriba),
    se sigue paginando hasta completar la letra, sin repetir lo ya leído.
    """
    crawl = LetterCrawl(crawler, letter=letter, page1=page1)
    k = crawl.expected_total - previous_total
    if k <= 0:
        raise ValueError(f"Letter {letter}: incremental load needs a larger total (k={k}).")

    crawl.fetch_until(k + 1)
    if len(crawl.rows) > k and row_values(crawl.rows[k]) == previous_first_values:
        new_rows = crawl.rows[:k]
        first = new_rows[0]
        logger.info("Letter %s -> incremental: %s new rows in %s pages, %s carried from "
                    "the last Bronze snapshot", letter, k, crawl.pages_read, previous_total)
        return LetterSnapshot(
            letter=letter,
            total_records=crawl.expected_total,
            total_pages=max(1, -(-crawl.expected_total // ROWS_PER_PAGE)),
            first_hash=row_hash_from_values(first[c] for c in SITE_FIELDS),
            first_values=row_values(first),
            rows=new_rows,
            carried_rows=previous_total,
        )

    logger.info("Letter %s -> last snapshot's first record is not at position %s "
                "(removals or non-top inclusions) -> full download", letter, k + 1)
    return crawl.finish()


# ==========================================================
# LECTURAS Y ESCRITURA
# ==========================================================
def ensure_tables(spark, catalog: str) -> None:
    """Sólo la tabla de Bronze: la ingesta no crea nada de Silver."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{BRONZE_SCHEMA}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{BRONZE_TABLE} (
            name STRING, debt_amount STRING, case_number STRING, court STRING,
            org_unit STRING, enforcement_agent STRING, termination_date STRING,
            inclusion_date STRING, reason STRING, letter STRING,
            page_number INT, position_in_page INT,
            ingestion_date DATE, dp_update_ts TIMESTAMP
        ) USING DELTA CLUSTER BY (letter, ingestion_date)
    """)


def read_control_totals(spark, catalog: str) -> dict[str, int]:
    """letter -> total_records de la versión vigente (comprobación 1)."""
    try:
        rows = spark.sql(f"""
            SELECT letter, total_records
              FROM {catalog}.{LETTER_CONTROL_TABLE}
             WHERE is_current
        """).collect()
    except Exception:
        return {}
    return {r["letter"]: int(r["total_records"]) for r in rows}


@dataclass(frozen=True)
class BronzeSnapshotInfo:
    first_values: tuple
    total_rows: int


def read_bronze_last_snapshots(spark, catalog: str) -> dict[str, BronzeSnapshotInfo]:
    """letter -> primer registro (9 campos crudos) y nº de filas del último snapshot."""
    cols = ", ".join(f"b.{c}" for c in SITE_FIELDS)
    try:
        rows = spark.sql(f"""
            SELECT letter, total_rows, {", ".join(SITE_FIELDS)}
              FROM (SELECT b.letter, b.page_number, b.position_in_page, {cols},
                           COUNT(*) OVER (PARTITION BY b.letter) AS total_rows
                      FROM {catalog}.{BRONZE_TABLE} b
                      JOIN (SELECT letter, MAX(dp_update_ts) AS ts
                              FROM {catalog}.{BRONZE_TABLE} GROUP BY letter) m
                        ON b.letter = m.letter AND b.dp_update_ts = m.ts)
             WHERE page_number = 1 AND position_in_page = 1
        """).collect()
    except Exception:
        return {}
    return {
        r["letter"]: BronzeSnapshotInfo(
            first_values=tuple((r[c] or "") for c in SITE_FIELDS),
            total_rows=int(r["total_rows"]),
        )
        for r in rows
    }


def silver_has_current_hash(spark, catalog: str, letter: str, row_hash: str) -> bool:
    """
    Lado Silver de la comprobación 2: ¿existe una versión VIGENTE de esta
    letra con este row_hash? Es pertenencia por hash, no búsqueda por
    posición: en SCD2 las posiciones quedan congeladas por versión.
    """
    try:
        row = spark.sql(f"""
            SELECT 1 FROM {catalog}.{SILVER_TABLE}
             WHERE letter = '{letter}' AND is_current AND row_hash = '{row_hash}'
             LIMIT 1
        """).first()
    except Exception:
        return False
    return row is not None


def write_letter_snapshot(spark, catalog: str, snapshot: LetterSnapshot) -> None:
    """
    Escribe el snapshot en Bronze, y nada más.

    Una sola sentencia inserta todas las filas de la letra, así que
    ingestion_date y dp_update_ts se evalúan una vez y todas las filas
    comparten instante: ese instante identifica el snapshot.

    En una carga incremental esa misma sentencia añade, detrás de las altas,
    las filas de la última foto de la letra desplazadas tantas posiciones como
    altas hay. Su row_hash no cambia (no depende de la posición), así que la
    transformación no las toca.
    """
    letter = snapshot.letter
    if not letter.isalpha() or len(letter) != 1:
        raise ValueError(f"Invalid letter: {letter!r}")

    if not snapshot.rows:
        logger.info("Letter %s loaded | 0 rows: nothing to write in Bronze", letter)
        return

    df = spark.createDataFrame(
        [tuple(r[c] for c in BRONZE_DATA_COLUMNS) for r in snapshot.rows],
        schema=BRONZE_DATA_DDL,
    )
    if snapshot.carried_rows:
        table = f"{catalog}.{BRONZE_TABLE}"
        shift = len(snapshot.rows)
        carried = spark.sql(f"""
            SELECT {", ".join(SITE_FIELDS)}, letter,
                   CAST(idx DIV {ROWS_PER_PAGE} + 1 AS INT) AS page_number,
                   CAST(idx % {ROWS_PER_PAGE} + 1 AS INT) AS position_in_page
              FROM (SELECT *,
                           (page_number - 1) * {ROWS_PER_PAGE} + position_in_page - 1 + {shift}
                               AS idx
                      FROM {table}
                     WHERE letter = '{letter}'
                       AND dp_update_ts = (SELECT MAX(dp_update_ts) FROM {table}
                                            WHERE letter = '{letter}'))
        """)
        df = df.unionByName(carried)
    df = (
        df.withColumn("ingestion_date", F.current_date())
        .withColumn("dp_update_ts", F.current_timestamp())
    )
    df.write.format("delta").mode("append").saveAsTable(f"{catalog}.{BRONZE_TABLE}")

    logger.info("Letter %s loaded | rows=%s (downloaded=%s carried=%s) pages=%s",
                letter, len(snapshot.rows) + snapshot.carried_rows, len(snapshot.rows),
                snapshot.carried_rows, snapshot.total_pages)


# ==========================================================
# BUCLE DE INGESTA
# ==========================================================
def run_ingestion(
    spark,
    *,
    catalog: str,
    settings: CrawlerSettings,
    letters: list[str],
    forced_letters: list[str] | None = None,
    control_loader=read_control_totals,
    bronze_loader=read_bronze_last_snapshots,
    silver_checker=silver_has_current_hash,
    snapshot_writer=write_letter_snapshot,
) -> dict:
    """
    Recorre las letras, decide cuáles hay que descargar y escribe Bronze.

    El borrado de lo no consolidado NO se hace aquí: lo hace el punto de
    entrada con utils_general.delete_records_after_ingestion_timestamp,
    antes de llamar a esta función.
    """
    letters = [x.upper() for x in letters]
    force = {x.upper() for x in (forced_letters or [])}

    control = control_loader(spark, catalog)
    bronze_last = bronze_loader(spark, catalog)
    logger.info("Current state -> letters in control: %s, snapshots in Bronze: %s",
                len(control), len(bronze_last))

    crawler = CitiusCrawler(logger=logger, settings=settings)

    loaded: list[str] = []
    loaded_incremental: list[str] = []
    skipped: list[str] = []
    skipped_pending: list[str] = []
    failed: dict[str, str] = {}

    try:
        for letter in letters:
            try:
                page1 = crawler.fetch_first_page(letter=letter, period=settings.period)
                site_total = int(page1.record_count or 0)
                fresh_values = first_record_values(page1.parsed_rows, site_total)
                fresh_hash = first_record_hash(page1.parsed_rows, site_total)

                stored_total = control.get(letter)
                bronze_snapshot = bronze_last.get(letter)
                bronze_values = bronze_snapshot.first_values if bronze_snapshot else None

                if letter in force:
                    reason = "forced"
                elif stored_total is None:
                    reason = "first load"
                elif stored_total != site_total:
                    reason = "total differs"
                elif fresh_values != bronze_values:
                    reason = "first record differs from Bronze"
                elif fresh_hash is None:
                    logger.info("Letter %s unchanged (empty) -> skipped", letter)
                    skipped.append(letter)
                    continue
                elif silver_checker(spark, catalog, letter, fresh_hash):
                    logger.info("Letter %s unchanged (total=%s) -> skipped", letter, site_total)
                    skipped.append(letter)
                    continue
                else:
                    # Tras el borrado previo, Bronze sólo contiene snapshots ya
                    # consolidados: si Silver no tiene ese primer registro es
                    # porque no puede representarlo (clave incompleta).
                    logger.warning(
                        "Letter %s: first record matches Bronze but is not current in "
                        "Silver (likely an incomplete key) -> skipped", letter)
                    skipped_pending.append(letter)
                    continue

                # Incremental sólo si la última foto de Bronze es la que refleja
                # letter_control (mismo nº de filas) y el contador ha crecido.
                can_be_incremental = (
                    letter not in force
                    and stored_total is not None
                    and bronze_snapshot is not None
                    and bronze_snapshot.total_rows == stored_total
                    and site_total > stored_total
                )
                if can_be_incremental:
                    logger.info("Letter %s -> trying incremental load (%s: %s -> %s)",
                                letter, reason, stored_total, site_total)
                    snapshot = crawl_letter_incremental(
                        crawler, letter=letter, page1=page1,
                        previous_total=stored_total,
                        previous_first_values=bronze_snapshot.first_values,
                    )
                else:
                    logger.info("Letter %s -> full download (%s)", letter, reason)
                    snapshot = crawl_letter(crawler, letter=letter, page1=page1)

                snapshot_writer(spark, catalog, snapshot)
                loaded.append(letter)
                if snapshot.carried_rows:
                    loaded_incremental.append(letter)

            except Exception as exc:
                logger.exception("Letter %s failed; nothing written for it.", letter)
                failed[letter] = str(exc)
            finally:
                crawler.polite_pause()
    finally:
        crawler.close()

    summary = {
        "loaded": loaded,
        "loaded_incremental": loaded_incremental,
        "skipped": skipped,
        "skipped_pending_silver": skipped_pending,
        "failed": failed,
    }
    logger.info("Letters -> loaded=%s (incremental=%s) skipped=%s pending_silver=%s failed=%s",
                len(loaded), len(loaded_incremental), len(skipped), len(skipped_pending),
                len(failed))

    if failed:
        raise RuntimeError(f"Ingestion finished with failed letters: {failed}")
    return summary

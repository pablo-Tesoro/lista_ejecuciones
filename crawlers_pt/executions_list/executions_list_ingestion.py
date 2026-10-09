# ── Logging ─────────────────────────────────────────────────────
from src.dataplatform_tenders.crawlers_pt.common.logging_config import setup_logging

import src.dataplatform_tenders.crawlers_pt.common.utils_general as utils_general
import src.dataplatform_tenders.crawlers_pt.executions_list.ingestion_utils as ingestion_utils
import src.dataplatform_tenders.crawlers_pt.executions_list.process_control as process_control
from src.dataplatform_tenders.crawlers_pt.executions_list.settings import CrawlerSettings
import logging

logger = logging.getLogger("dataplatform_tenders.crawlers_pt.executions_list_ingestion")


def main() -> None:

    # ── Initialize logging (only at the entry point) ────────────
    setup_logging("executions_list_ingestion")

    logger.info("=" * 60)
    logger.info("START - Executions List Ingestion")
    logger.info("=" * 60)

    # ── Parameters ──────────────────────────────────────────────
    # letters: las letras de esta tarea ("A,M,Z"), o "ALL" para todas.
    kwargs = ["catalog", "env", "letters"]
    args = utils_general.get_args(kwargs)
    catalog = args.catalog
    env = args.env
    logger.info("Parameters received -> catalog: %s, env: %s, letters: %s",
                catalog, env, args.letters)

    # ── Configuration ───────────────────────────────────────────
    logger.info("Reading configuration files...")
    config_general_executions_list = utils_general.read_config(
        step="executions_list", filename="config_general_executions_list.json"
    )
    config_env = config_general_executions_list[env]
    settings = CrawlerSettings.from_config(config_env)
    letters = ingestion_utils.select_letters(args.letters, config_env["letters"])
    forced_letters = config_env.get("forced_letters", [])
    logger.info("Configuration loaded successfully -> url: %s, period: %s, letters: %s",
                settings.url, settings.period, ",".join(letters))

    # ── Process ─────────────────────────────────────────────────
    logger.info("Consulting last time processed...")
    last_time_processed = process_control.read_watermark(spark, catalog)
    logger.info("Last time processed: %s", process_control.watermark_literal(last_time_processed))

    logger.info("Deleting unprocessed records of letters %s...", ",".join(letters))
    ingestion_utils.delete_unconsolidated_snapshots(
        spark, catalog, letters=letters, watermark=last_time_processed
    )
    logger.info("deleted unprocessed records")

    logger.info("Ingesting letters...")
    summary = ingestion_utils.run_ingestion(
        spark,
        catalog=catalog,
        settings=settings,
        letters=letters,
        forced_letters=forced_letters,
    )

    # ── Final summary ───────────────────────────────────────────
    logger.info("Loaded letters: %s", summary["loaded"] or "none")
    logger.info("  of which incremental: %s", summary["loaded_incremental"] or "none")
    logger.info("Skipped letters: %s", summary["skipped"] or "none")
    logger.info("=" * 60)
    logger.info("END - Executions List Ingestion")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

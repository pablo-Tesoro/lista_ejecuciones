# ── Logging ─────────────────────────────────────────────────────
from src.dataplatform_tenders.crawlers_pt.common.logging_config import setup_logging

import src.dataplatform_tenders.crawlers_pt.common.utils_general as utils_general
import src.dataplatform_tenders.crawlers_pt.executions_list.gold_utils as gold_utils
import logging

from pyspark.sql import SparkSession

logger = logging.getLogger("dataplatform_tenders.crawlers_pt.executions_list_gold")


def main() -> None:

    # ── Initialize logging (only at the entry point) ────────────
    setup_logging("executions_list_gold")

    logger.info("=" * 60)
    logger.info("START - Executions List Gold")
    logger.info("=" * 60)

    # ── Parameters ──────────────────────────────────────────────
    args = utils_general.get_args(["catalog"])
    catalog = args.catalog
    logger.info("Parameters received -> catalog: %s", catalog)

    # ── Process ─────────────────────────────────────────────────
    spark = SparkSession.builder.getOrCreate()
    summary = gold_utils.run_gold(spark, catalog=catalog)

    # ── Final summary ───────────────────────────────────────────
    logger.info("Published -> execution_list: %s rows, letter_control: %s letters",
                summary["execution_list_rows"], summary["letter_control_rows"])
    logger.info("DQ warnings: %s", summary["failed_warning"] or "none")
    logger.info("=" * 60)
    logger.info("END - Executions List Gold")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

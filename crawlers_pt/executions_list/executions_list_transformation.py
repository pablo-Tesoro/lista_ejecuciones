# ── Logging ─────────────────────────────────────────────────────
from src.dataplatform_tenders.crawlers_pt.common.logging_config import setup_logging

import src.dataplatform_tenders.crawlers_pt.common.utils_general as utils_general
import src.dataplatform_tenders.crawlers_pt.executions_list.transformation_utils as transformation_utils
import logging

logger = logging.getLogger("dataplatform_tenders.crawlers_pt.executions_list_transformation")


def main() -> None:

    # ── Initialize logging (only at the entry point) ────────────
    setup_logging("executions_list_transformation")

    logger.info("=" * 60)
    logger.info("START - Executions List Transformation")
    logger.info("=" * 60)

    # ── Parameters ──────────────────────────────────────────────
    args = utils_general.get_args(["catalog"])
    catalog = args.catalog
    logger.info("Parameters received -> catalog: %s", catalog)

    # ── Process ─────────────────────────────────────────────────
    transformation_utils.run_transformation(spark, catalog=catalog)

    logger.info("=" * 60)
    logger.info("END - Executions List Transformation")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()

"""
pipeline.py
-----------
End-to-end orchestrator for the Indian Startup Funding Data Pipeline.

Runs, in strict sequence:
    1. Bronze layer  (raw ingestion)
    2. Silver layer  (cleaning & standardization)
    3. Gold layer    (business analytics tables)

Usage
-----
    python src/pipeline.py

A single SparkSession is created once and shared across all three layers
for efficiency, then stopped at the end.
"""

import sys
import time
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.spark_session import get_spark_session, stop_spark_session  # noqa: E402
from src.utils import get_logger  # noqa: E402
from src.bronze import run_bronze_layer  # noqa: E402
from src.silver import run_silver_layer  # noqa: E402
from src.gold import run_gold_layer  # noqa: E402

logger = get_logger(__name__)


def run_pipeline() -> None:
    """Run the full Bronze -> Silver -> Gold pipeline end to end."""
    pipeline_start = time.time()
    logger.info("################################################")
    logger.info("#   INDIAN STARTUP FUNDING PIPELINE - STARTING   #")
    logger.info("################################################")

    spark = get_spark_session()

    try:
        stage_start = time.time()
        bronze_df = run_bronze_layer(spark)
        logger.info("Bronze layer finished in %.2f seconds.", time.time() - stage_start)

        stage_start = time.time()
        silver_df = run_silver_layer(spark, bronze_df=bronze_df)
        logger.info("Silver layer finished in %.2f seconds.", time.time() - stage_start)

        stage_start = time.time()
        gold_tables = run_gold_layer(spark, silver_df=silver_df)
        logger.info("Gold layer finished in %.2f seconds.", time.time() - stage_start)

        logger.info("Gold tables produced: %s", ", ".join(gold_tables.keys()))

    except Exception:
        logger.exception("Pipeline failed with an unhandled exception.")
        raise
    finally:
        stop_spark_session(spark)

    total_time = time.time() - pipeline_start
    logger.info("################################################")
    logger.info("#   PIPELINE COMPLETE - Total time: %.2f sec   #", total_time)
    logger.info("################################################")


if __name__ == "__main__":
    run_pipeline()

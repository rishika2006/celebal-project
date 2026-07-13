"""
bronze.py
---------
Bronze Layer: Raw ingestion.

Responsibility of this layer ONLY:
    - Read the raw CSV file exactly as it is.
    - Do NOT clean, transform, deduplicate or standardize anything here.
    - Persist the raw data as-is to Parquet so downstream layers always
      have an immutable, replayable copy of the original source data.
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import config  # noqa: E402
from src.spark_session import get_spark_session, stop_spark_session  # noqa: E402
from src.utils import get_logger  # noqa: E402

from pyspark.sql import DataFrame, SparkSession

logger = get_logger(__name__)


def read_raw_csv(spark: SparkSession, csv_path: Path = config.RAW_CSV_PATH) -> DataFrame:
    """
    Read the raw source CSV file with every column typed as string.
    Reading everything as string is intentional at Bronze: type-casting
    and cleaning are Silver-layer responsibilities, not Bronze's.
    """
    logger.info("Reading raw CSV from: %s", csv_path)

    if not csv_path.exists():
        raise FileNotFoundError(
            f"Raw CSV not found at {csv_path}. Place the source file there before running the pipeline."
        )

    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")  # keep everything as string at Bronze
        .option("multiLine", "true")
        .option("escape", '"')
        .csv(str(csv_path))
    )

    row_count = df.count()
    logger.info("Raw CSV read successfully. Rows: %s, Columns: %s", row_count, len(df.columns))
    return df


def write_bronze_parquet(df: DataFrame, output_path: Path = config.BRONZE_TABLE_PATH) -> None:
    """Persist the raw (untouched) dataframe to the Bronze Parquet location."""
    logger.info("Writing Bronze parquet to: %s", output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write.mode("overwrite").parquet(str(output_path))
    logger.info("Bronze layer write complete.")


def run_bronze_layer(spark: SparkSession = None) -> DataFrame:
    """
    Orchestrates the full Bronze step: read raw CSV -> write raw Parquet.
    Returns the bronze dataframe so pipeline.py can chain it into Silver
    without a second Spark read, if desired.
    """
    owns_spark = spark is None
    spark = spark or get_spark_session()

    logger.info("===== BRONZE LAYER START =====")
    config.ensure_directories_exist()

    raw_df = read_raw_csv(spark)
    write_bronze_parquet(raw_df)

    logger.info("===== BRONZE LAYER COMPLETE =====")

    if owns_spark:
        stop_spark_session(spark)

    return raw_df


if __name__ == "__main__":
    run_bronze_layer()

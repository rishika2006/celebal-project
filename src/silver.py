"""
silver.py
---------
Silver Layer: Cleaning, standardization and conformance.

Responsibilities:
    - Rename raw columns into clean, snake_case names.
    - Remove exact duplicate records.
    - Handle null values.
    - Standardize city, investor and startup names.
    - Clean the funding amount field into a proper numeric USD column.
    - Convert the date field (multiple raw formats) into a real DateType.
    - Fix data types across the board.
    - Remove invalid records (missing keys, non-positive/unparseable
      amounts, unparseable dates).
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import config  # noqa: E402
from src.spark_session import get_spark_session, stop_spark_session  # noqa: E402
from src.utils import (  # noqa: E402
    get_logger,
    standardize_city_column,
    standardize_investor_column,
    standardize_startup_name_column,
    clean_funding_amount_column,
    parse_flexible_date_column,
    null_if_blank,
)

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = get_logger(__name__)

# Mapping from raw (messy) CSV headers to clean, snake_case Silver column names
RAW_TO_SILVER_COLUMNS = {
    "SNo": "record_id",
    "Startup Name": "startup_name",
    "Industry/Sector": "sector",
    "City": "city",
    "Investors Name": "investors_name",
    "InvestmentType": "investment_type",
    "Amount(USD)": "amount_usd",
    "Date": "date",
}


def rename_columns(df: DataFrame) -> DataFrame:
    """Rename raw CSV headers into clean, consistent snake_case names."""
    for raw_name, silver_name in RAW_TO_SILVER_COLUMNS.items():
        if raw_name in df.columns:
            df = df.withColumnRenamed(raw_name, silver_name)
    return df


def cast_record_id(df: DataFrame) -> DataFrame:
    """Fix the data type of record_id -> integer."""
    return df.withColumn("record_id", F.col("record_id").cast("int"))


def trim_blank_strings(df: DataFrame) -> DataFrame:
    """Convert empty/whitespace-only strings to null across all key text columns."""
    for column in ("startup_name", "sector", "city", "investors_name", "investment_type"):
        if column in df.columns:
            df = null_if_blank(df, column)
    return df


def deduplicate_records(df: DataFrame) -> DataFrame:
    """Remove exact duplicate rows (a known issue injected in the raw source)."""
    before = df.count()
    df = df.dropDuplicates()
    after = df.count()
    logger.info("Deduplication removed %s exact duplicate rows (%s -> %s).", before - after, before, after)
    return df


def handle_nulls(df: DataFrame) -> DataFrame:
    """
    Handle nulls per business rule:
        - sector: unknowns become 'Unknown'
        - investment_type: unknowns become 'Undisclosed'
        - startup_name / city / investors_name / amount_usd / date:
          these are business-critical -> rows missing them are dropped
          later in `remove_invalid_records`, not defaulted, to avoid
          skewing analytics with fake values.
    """
    df = df.fillna({"sector": "Unknown", "investment_type": "Undisclosed"})
    return df


def standardize_all(df: DataFrame) -> DataFrame:
    """Apply every standardization helper (city, investor, startup name)."""
    df = standardize_startup_name_column(df, "startup_name")
    df = standardize_city_column(df, "city")
    df = standardize_investor_column(df, "investors_name")
    return df


def clean_amount_and_date(df: DataFrame) -> DataFrame:
    """Clean the funding amount and parse the date column."""
    df = clean_funding_amount_column(df, "amount_usd", "amount_usd")
    df = parse_flexible_date_column(df, "date", "date")
    return df


def add_derived_columns(df: DataFrame) -> DataFrame:
    """Add year / month / quarter columns used repeatedly by the Gold layer."""
    df = (
        df.withColumn("funding_year", F.year("date"))
        .withColumn("funding_month", F.month("date"))
        .withColumn("funding_quarter", F.quarter("date"))
    )
    return df


def remove_invalid_records(df: DataFrame) -> DataFrame:
    """
    Drop records that cannot be trusted for analytics:
        - missing startup_name, city or investors_name
        - amount_usd is null or below the configured minimum threshold
        - date could not be parsed into a valid DateType
    """
    before = df.count()
    df = df.filter(
        F.col("startup_name").isNotNull()
        & F.col("city").isNotNull()
        & F.col("investors_name").isNotNull()
        & F.col("amount_usd").isNotNull()
        & (F.col("amount_usd") >= F.lit(config.MIN_VALID_FUNDING_AMOUNT_USD))
        & F.col("date").isNotNull()
    )
    after = df.count()
    logger.info("Invalid-record removal dropped %s rows (%s -> %s).", before - after, before, after)
    return df


def transform_bronze_to_silver(bronze_df: DataFrame) -> DataFrame:
    """Full Bronze -> Silver transformation chain, in the required order."""
    df = rename_columns(bronze_df)
    df = cast_record_id(df)
    df = trim_blank_strings(df)
    df = deduplicate_records(df)
    df = handle_nulls(df)
    df = standardize_all(df)
    df = clean_amount_and_date(df)
    df = add_derived_columns(df)
    df = remove_invalid_records(df)
    return df


def write_silver_parquet(df: DataFrame, output_path: Path = config.SILVER_TABLE_PATH) -> None:
    """Persist the cleaned dataframe to the Silver Parquet location."""
    logger.info("Writing Silver parquet to: %s", output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write.mode("overwrite").partitionBy("funding_year").parquet(str(output_path))
    logger.info("Silver layer write complete.")


def read_bronze_parquet(spark: SparkSession, path: Path = config.BRONZE_TABLE_PATH) -> DataFrame:
    """Read the Bronze parquet table back from disk (used when Silver runs standalone)."""
    if not path.exists():
        raise FileNotFoundError(f"Bronze parquet not found at {path}. Run the Bronze layer first.")
    return spark.read.parquet(str(path))


def run_silver_layer(spark: SparkSession = None, bronze_df: DataFrame = None) -> DataFrame:
    """
    Orchestrates the full Silver step. Accepts an optional bronze_df so
    pipeline.py can chain Bronze -> Silver without re-reading from disk.
    """
    owns_spark = spark is None
    spark = spark or get_spark_session()

    logger.info("===== SILVER LAYER START =====")
    config.ensure_directories_exist()

    if bronze_df is None:
        bronze_df = read_bronze_parquet(spark)

    silver_df = transform_bronze_to_silver(bronze_df)
    write_silver_parquet(silver_df)

    logger.info("===== SILVER LAYER COMPLETE =====")

    if owns_spark:
        stop_spark_session(spark)

    return silver_df


if __name__ == "__main__":
    run_silver_layer()

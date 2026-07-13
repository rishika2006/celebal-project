"""
gold.py
-------
Gold Layer: Business-ready analytical tables, built with Spark SQL on top
of the cleaned Silver table.

Each function below builds exactly one Gold table and returns it as a
DataFrame. `run_gold_layer` builds all of them and writes each to its own
Parquet location (see config.GOLD_TABLES), and also registers a temp view
so `sql/analysis.sql` and `sql/business_queries.sql` can be run directly
against the same session for ad-hoc analysis.
"""

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import config  # noqa: E402
from src.spark_session import get_spark_session, stop_spark_session  # noqa: E402
from src.utils import get_logger  # noqa: E402

from pyspark.sql import DataFrame, SparkSession

logger = get_logger(__name__)

SILVER_VIEW_NAME = "silver_startup_funding"


def read_silver_parquet(spark: SparkSession, path: Path = config.SILVER_TABLE_PATH) -> DataFrame:
    """Read the Silver parquet table back from disk (used when Gold runs standalone)."""
    if not path.exists():
        raise FileNotFoundError(f"Silver parquet not found at {path}. Run the Silver layer first.")
    return spark.read.parquet(str(path))


def register_silver_view(spark: SparkSession, silver_df: DataFrame) -> None:
    """Register the Silver dataframe as a Spark SQL temp view for use by .sql files."""
    silver_df.createOrReplaceTempView(SILVER_VIEW_NAME)
    logger.info("Registered Spark SQL temp view: %s", SILVER_VIEW_NAME)


# ---------------------------------------------------------------------------
# Gold table builders (Spark SQL)
# ---------------------------------------------------------------------------
def build_top_funded_startups(spark: SparkSession) -> DataFrame:
    """Top startups ranked by total funding raised."""
    return spark.sql(
        f"""
        SELECT
            startup_name,
            ROUND(SUM(amount_usd), 2) AS total_funding_usd,
            COUNT(*) AS number_of_rounds
        FROM {SILVER_VIEW_NAME}
        GROUP BY startup_name
        ORDER BY total_funding_usd DESC
        """
    )


def build_top_investors(spark: SparkSession) -> DataFrame:
    """Top investors ranked by total capital deployed (investor names exploded)."""
    return spark.sql(
        f"""
        SELECT
            investor,
            ROUND(SUM(amount_usd), 2) AS total_invested_usd,
            COUNT(DISTINCT startup_name) AS startups_invested_in
        FROM (
            SELECT
                EXPLODE(SPLIT(investors_name, ',\\\\s*')) AS investor,
                startup_name,
                amount_usd
            FROM {SILVER_VIEW_NAME}
        )
        GROUP BY investor
        ORDER BY total_invested_usd DESC
        """
    )


def build_top_sectors(spark: SparkSession) -> DataFrame:
    """Top sectors ranked by total funding raised."""
    return spark.sql(
        f"""
        SELECT
            sector,
            ROUND(SUM(amount_usd), 2) AS total_funding_usd,
            COUNT(*) AS number_of_deals
        FROM {SILVER_VIEW_NAME}
        GROUP BY sector
        ORDER BY total_funding_usd DESC
        """
    )


def build_top_cities(spark: SparkSession) -> DataFrame:
    """Top cities ranked by total funding raised."""
    return spark.sql(
        f"""
        SELECT
            city,
            ROUND(SUM(amount_usd), 2) AS total_funding_usd,
            COUNT(*) AS number_of_deals
        FROM {SILVER_VIEW_NAME}
        GROUP BY city
        ORDER BY total_funding_usd DESC
        """
    )


def build_funding_by_year(spark: SparkSession) -> DataFrame:
    """Total funding amount and deal count grouped by year."""
    return spark.sql(
        f"""
        SELECT
            funding_year,
            ROUND(SUM(amount_usd), 2) AS total_funding_usd,
            COUNT(*) AS number_of_deals
        FROM {SILVER_VIEW_NAME}
        GROUP BY funding_year
        ORDER BY funding_year
        """
    )


def build_funding_by_investment_type(spark: SparkSession) -> DataFrame:
    """Total funding amount grouped by investment/round type (Seed, Series A, etc.)."""
    return spark.sql(
        f"""
        SELECT
            investment_type,
            ROUND(SUM(amount_usd), 2) AS total_funding_usd,
            COUNT(*) AS number_of_deals
        FROM {SILVER_VIEW_NAME}
        GROUP BY investment_type
        ORDER BY total_funding_usd DESC
        """
    )


def build_investor_activity(spark: SparkSession) -> DataFrame:
    """Number of deals participated in per investor, plus average ticket size."""
    return spark.sql(
        f"""
        SELECT
            investor,
            COUNT(*) AS number_of_deals,
            ROUND(AVG(amount_usd), 2) AS avg_ticket_size_usd
        FROM (
            SELECT
                EXPLODE(SPLIT(investors_name, ',\\\\s*')) AS investor,
                amount_usd
            FROM {SILVER_VIEW_NAME}
        )
        GROUP BY investor
        ORDER BY number_of_deals DESC
        """
    )


def build_sector_funding(spark: SparkSession) -> DataFrame:
    """Sector funding broken down by year (sector x year matrix as rows)."""
    return spark.sql(
        f"""
        SELECT
            sector,
            funding_year,
            ROUND(SUM(amount_usd), 2) AS total_funding_usd
        FROM {SILVER_VIEW_NAME}
        GROUP BY sector, funding_year
        ORDER BY sector, funding_year
        """
    )


def build_monthly_funding(spark: SparkSession) -> DataFrame:
    """Total funding amount grouped by year and month (time series)."""
    return spark.sql(
        f"""
        SELECT
            funding_year,
            funding_month,
            ROUND(SUM(amount_usd), 2) AS total_funding_usd,
            COUNT(*) AS number_of_deals
        FROM {SILVER_VIEW_NAME}
        GROUP BY funding_year, funding_month
        ORDER BY funding_year, funding_month
        """
    )


def build_average_investment(spark: SparkSession) -> DataFrame:
    """Overall and per-investment-type average investment size."""
    return spark.sql(
        f"""
        SELECT
            investment_type,
            ROUND(AVG(amount_usd), 2) AS avg_investment_usd,
            ROUND(MIN(amount_usd), 2) AS min_investment_usd,
            ROUND(MAX(amount_usd), 2) AS max_investment_usd
        FROM {SILVER_VIEW_NAME}
        GROUP BY investment_type
        ORDER BY avg_investment_usd DESC
        """
    )


GOLD_BUILDERS = {
    "top_funded_startups": build_top_funded_startups,
    "top_investors": build_top_investors,
    "top_sectors": build_top_sectors,
    "top_cities": build_top_cities,
    "funding_by_year": build_funding_by_year,
    "funding_by_investment_type": build_funding_by_investment_type,
    "investor_activity": build_investor_activity,
    "sector_funding": build_sector_funding,
    "monthly_funding": build_monthly_funding,
    "average_investment": build_average_investment,
}


def write_gold_table(df: DataFrame, table_name: str) -> None:
    """Write a single Gold table to its configured Parquet location."""
    output_path = config.GOLD_TABLES[table_name]
    logger.info("Writing Gold table '%s' to: %s", table_name, output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write.mode("overwrite").parquet(str(output_path))


def run_gold_layer(spark: SparkSession = None, silver_df: DataFrame = None) -> dict:
    """
    Orchestrates the full Gold step: build every analytical table defined
    in GOLD_BUILDERS and persist each one to its own Parquet folder.
    Returns a dict of {table_name: DataFrame} for further use (e.g. tests,
    notebooks, or the pipeline printing a summary).
    """
    owns_spark = spark is None
    spark = spark or get_spark_session()

    logger.info("===== GOLD LAYER START =====")
    config.ensure_directories_exist()

    if silver_df is None:
        silver_df = read_silver_parquet(spark)

    register_silver_view(spark, silver_df)

    results = {}
    for table_name, builder_fn in GOLD_BUILDERS.items():
        logger.info("Building Gold table: %s", table_name)
        table_df = builder_fn(spark).cache()
        table_df.count()  # materialize cache
        write_gold_table(table_df, table_name)
        results[table_name] = table_df

    logger.info("===== GOLD LAYER COMPLETE (%s tables built) =====", len(results))

    if owns_spark:
        stop_spark_session(spark)

    return results


if __name__ == "__main__":
    run_gold_layer()

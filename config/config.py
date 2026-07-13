"""
config.py
---------
Centralized configuration for the Indian Startup Funding Data Pipeline.

All file paths, Spark settings and constants used across the Bronze,
Silver and Gold layers are defined here so that no other module needs to
hard-code a path. Uses pathlib for cross-platform (Windows/Linux/Mac)
compatibility as required by the project standards.
"""

from pathlib import Path

# ---------------------------------------------------------------------------
# Project root: two levels up from this file (config/config.py -> project root)
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------------------
# Data directories (Medallion Architecture layers)
# ---------------------------------------------------------------------------
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

# ---------------------------------------------------------------------------
# Logs directory
# ---------------------------------------------------------------------------
LOGS_DIR = PROJECT_ROOT / "logs"

# ---------------------------------------------------------------------------
# SQL directory
# ---------------------------------------------------------------------------
SQL_DIR = PROJECT_ROOT / "sql"

# ---------------------------------------------------------------------------
# Source file name(s)
# ---------------------------------------------------------------------------
RAW_CSV_FILE_NAME = "indian_startup_funding_2020_2025_sample.csv"
RAW_CSV_PATH = RAW_DIR / RAW_CSV_FILE_NAME

# ---------------------------------------------------------------------------
# Bronze / Silver / Gold table (parquet folder) names
# ---------------------------------------------------------------------------
BRONZE_TABLE_NAME = "bronze_startup_funding"
BRONZE_TABLE_PATH = BRONZE_DIR / BRONZE_TABLE_NAME

SILVER_TABLE_NAME = "silver_startup_funding"
SILVER_TABLE_PATH = SILVER_DIR / SILVER_TABLE_NAME

# Gold layer analytical tables
GOLD_TABLES = {
    "top_funded_startups": GOLD_DIR / "top_funded_startups",
    "top_investors": GOLD_DIR / "top_investors",
    "top_sectors": GOLD_DIR / "top_sectors",
    "top_cities": GOLD_DIR / "top_cities",
    "funding_by_year": GOLD_DIR / "funding_by_year",
    "funding_by_investment_type": GOLD_DIR / "funding_by_investment_type",
    "investor_activity": GOLD_DIR / "investor_activity",
    "sector_funding": GOLD_DIR / "sector_funding",
    "monthly_funding": GOLD_DIR / "monthly_funding",
    "average_investment": GOLD_DIR / "average_investment",
}

# ---------------------------------------------------------------------------
# Spark configuration
# ---------------------------------------------------------------------------
SPARK_APP_NAME = "IndianStartupFundingPipeline"
SPARK_MASTER = "local[*]"
SPARK_SHUFFLE_PARTITIONS = "4"  # small dataset -> keep partition count low locally

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
LOG_FILE_NAME = "pipeline.log"
LOG_FILE_PATH = LOGS_DIR / LOG_FILE_NAME
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"

# ---------------------------------------------------------------------------
# Data quality thresholds / business constants
# ---------------------------------------------------------------------------
MIN_VALID_FUNDING_AMOUNT_USD = 1000  # rows below this after cleaning are dropped
CURRENCY_INR_TO_USD_RATE = 1 / 83.0  # approximate conversion used for cleaning


def ensure_directories_exist() -> None:
    """Create every directory this project depends on if it does not exist yet."""
    for directory in (RAW_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR, LOGS_DIR, SQL_DIR):
        directory.mkdir(parents=True, exist_ok=True)
